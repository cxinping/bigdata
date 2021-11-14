# -*- coding: utf-8 -*-
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED, as_completed
import pandas as pd
import numpy as np
import time
import threading
from report.commons.connect_kudu import prod_execute_sql, dis_connection
from report.commons.db_helper import query_kudu_data
from report.commons.logging import get_logger
from report.commons.tools import list_of_groups
import os
import signal

log = get_logger(__name__)

"""
https://blog.csdn.net/lzx159951/article/details/104357909

异常值：一组测定值中与平均值的偏差超过两倍标准差的测定值

"""

import sys

sys.path.append('/you_filed_algos/app')

dest_dir = '/you_filed_algos/prod_kudu_data/checkpoint14'
no_plane_dest_file = dest_dir + '/check_14_no_plane_data.txt'
plane_dest_file = dest_dir + '/check_14_plane_data.txt'

conn_type = 'test'


def check_14_plane_data():
    """
    是飞机的交通费
    通过对比出发地与目的地相同或相近的报销项目，关注交通费偏离平均值或大多数人费用分布的较大情况。

     select finance_travel_id,bill_id,plane_beg_date, plane_end_date,plane_origin_name, plane_destin_name, plane_check_amount from 01_datamart_layer_007_h_cw_df.finance_travel_bill
     WHERE plane_check_amount > 0 AND isPlane = 'plane' AND ( plane_origin_name is not null AND plane_destin_name is not null)

    """
    init_file(plane_dest_file)

    columns_ls = ['finance_travel_id', 'bill_id', 'plane_beg_date', 'plane_end_date', 'plane_origin_name',
                  'plane_destin_name',
                  'plane_check_amount']
    columns_str = ",".join(columns_ls)

    sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill WHERE plane_check_amount > 0 AND isPlane = 'plane' AND ( plane_origin_name is not null AND plane_destin_name is not null) AND (plane_beg_date is not null AND plane_beg_date !='') limit 100001".format(
        columns_str=columns_str)

    count_sql = 'select count(a.finance_travel_id) from ({sql}) a'.format(sql=sql)
    log.info(count_sql)
    records = prod_execute_sql(conn_type='test', sqltype='select', sql=count_sql)
    count_records = records[0][0]

    max_size = 1 * 100000
    limit_size = 1 * 10000
    select_sql_ls = []

    log.info(f'* count_records ==> {count_records}')
    if count_records >= max_size:
        offset_size = 0
        while offset_size <= count_records:
            if offset_size + limit_size > count_records:
                limit_size = count_records - offset_size
                tmp_sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill WHERE plane_check_amount > 0 AND isPlane = 'plane' AND ( plane_origin_name is not null AND plane_destin_name is not null) AND (plane_beg_date is not null AND plane_beg_date !='') ORDER BY plane_beg_date limit {limit_size} offset {offset_size}".format(
                    columns_str=columns_str, limit_size=limit_size, offset_size=offset_size)

                select_sql_ls.append(tmp_sql)
                break
            else:
                tmp_sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill WHERE plane_check_amount > 0 AND isPlane = 'plane' AND ( plane_origin_name is not null AND plane_destin_name is not null) AND (plane_beg_date is not null AND plane_beg_date !='') ORDER BY plane_beg_date limit {limit_size} offset {offset_size}".format(
                    columns_str=columns_str, limit_size=limit_size, offset_size=offset_size)
                select_sql_ls.append(tmp_sql)

            offset_size = offset_size + limit_size
    else:
        tmp_sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill WHERE plane_check_amount > 0 AND isPlane = 'plane' AND ( plane_origin_name is not null AND plane_destin_name is not null) AND (plane_beg_date is not null AND plane_beg_date !='') ".format(
            columns_str=columns_str)
        select_sql_ls.append(tmp_sql)

    log.info('* 开始分页查询')

    # threadPool = ThreadPoolExecutor(max_workers=20, thread_name_prefix="thr")
    with ThreadPoolExecutor(max_workers=3, thread_name_prefix="thr") as threadPool:
        start_time = time.perf_counter()
        all_task = [threadPool.submit(exec_plane_task, sel_sql, plane_dest_file) for sel_sql in select_sql_ls]
        # wait(all_task, return_when=ALL_COMPLETED)

        done_futures, not_done_futures = wait(all_task, 0.2, return_when="ALL_COMPLETED")
        print(f"1 已完成：{done_futures}")
        print(f"2 未完成：{not_done_futures}")

        print('* waite threadPool completed')

        for future in as_completed(all_task):
            threadPool.shutdown()

    print('* All tasks complete')
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* 查询耗时 {consumed_time} sec')


parent_pid = 0

import psutil


def kill_pid():
    parent = psutil.Process(parent_pid)
    for child in parent.children(recursive=True):  # or parent.children() for recursive=False
        child.kill()
    parent.kill()


def exec_plane_task(sql, dest_file):  # dest_file
    try:
        records = prod_execute_sql(conn_type='test', sqltype='select', sql=sql)
        time.sleep(0.01)
        global parent_pid
        parent_pid = os.getpid()
        if records and len(records) > 0:
            for idx, record in enumerate(records):
                finance_travel_id = str(record[0])  # finance_travel_id
                bill_id = str(record[1])  # bill_id
                plane_beg_date = str(record[2])  # 飞机开始时间
                plane_end_date = str(record[3])  # 飞机结束时间
                plane_origin_name = str(record[4])  # 飞机出发地
                plane_destin_name = str(record[5])  # 飞机目的地
                plane_check_amount = str(record[6])  # 飞机票的费用

                plane_origin_name = plane_origin_name.replace(',', ' ')
                plane_destin_name = plane_destin_name.replace(',', ' ')

                record_str = f'{finance_travel_id},{bill_id},{plane_beg_date},{plane_end_date},{plane_origin_name},{plane_destin_name},{plane_check_amount}'

                # log.info(f'dest_file = {dest_file}')
                log.info(f"{threading.current_thread().name} is running")
                log.info(f'线程号 => {os.getpid()}')
                log.info(record_str)
                print()

                with open(dest_file, "a+", encoding='utf-8') as file:
                    file.write(record_str + "\n")

        return True
    except Exception as e:
        print(e)
        return False


def init_file(dest_file):
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)

    if os.path.exists(dest_file):
        os.remove(dest_file)


def analyze_plane_data(coefficient=2):
    """
    只包括飞机票费用

    :param coefficient: 系数，默认值为2
    :return:
    """

    print('======= analyze_plane_data ===========')

    rd_df = pd.read_csv(plane_dest_file, sep=',', header=None, encoding="utf-8",
                        dtype={'finance_travel_id': str, 'bill_id': str, 'plane_beg_date': str, 'plane_end_date': str,
                               'plane_origin_name': str, 'plane_destin_name': str, 'jour_amount': np.float64},
                        names=['finance_travel_id', 'bill_id', 'plane_beg_date', 'plane_end_date', 'plane_origin_name',
                               'plane_destin_name', 'plane_check_amount'])

    # print(rd_df.dtypes)
    rd_df = rd_df[:1500]
    print(rd_df.head(10))

    grouped_df = rd_df.groupby(['plane_beg_date', 'plane_origin_name', 'plane_destin_name'])
    # grouped_df = rd_df.groupby([ 'plane_origin_name', 'plane_destin_name'])
    print('=' * 60)

    bill_id_ls = []
    for name, group_df in grouped_df:
        # print(name)
        plane_beg_date, plane_origin_name, plane_destin_name = name
        temp = group_df.describe()[['plane_check_amount']]
        std_val = temp.at['std', 'plane_check_amount']  # 标准差
        mean_val = temp.at['mean', 'plane_check_amount']  # 平均值

        if std_val == 0 or np.isnan(std_val):
            std_val = 0

        # 数据的正常范围为 【mean - 2 × std , mean + 2 × std】
        max_val = mean_val + coefficient * std_val
        min_val = mean_val - coefficient * std_val

        if len(group_df) >= 2:
            # str_val = f'plane_beg_date={plane_beg_date}, plane_origin_name={plane_origin_name}, plane_destin_name={plane_destin_name}, 每组数据的数量={len(group_df)}, coefficient系数为 {coefficient}, 标准差={std_val},平均值={mean_val}, 数据的正常范围为 {min_val} 到 {max_val}'
            # print(str_val)
            # print(group_df)
            # print('')

            for index, row in group_df.iterrows():
                plane_check_amount = row['plane_check_amount']
                if plane_check_amount > max_val or plane_check_amount < min_val:
                    bill_id = row['bill_id']
                    bill_id_ls.append(bill_id)

    if bill_id_ls and len(bill_id_ls) >0:
        exec_sql(bill_id_ls)


def exec_sql(bill_id_ls):
    print('exec_sql ==> ', len(bill_id_ls))

    if bill_id_ls and len(bill_id_ls) > 0:
        group_ls = list_of_groups(bill_id_ls, 1000)
        # print(len(group_ls), group_ls)

        condition_sql = ''
        in_codition = 'bill_id IN {temp}'

        for idx, group in enumerate(group_ls):
            temp = in_codition.format(temp=str(tuple(group)))
            if idx == 0:
                condition_sql = temp
            else:
                condition_sql = condition_sql + ' OR ' + temp

        # print(condition_sql)

    sql = """
    INSERT INTO analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets
        SELECT uuid() as finance_id,
        bill_id ,
        '14' as unusual_id ,
         ' ' as company_code ,
         ' ' as account_period ,
         ' ' as finance_number ,
         ' ' as cost_center ,
         ' ' as profit_center ,
         ' ' as cart_head ,
         ' ' as bill_code ,
         ' ' as bill_beg_date ,
         ' ' as bill_end_date ,
         ' ' as origin_city ,
         ' ' as destin_city ,
         ' ' as beg_date ,
         ' ' as end_date ,
         ' ' as apply_emp_name ,
         ' ' as emp_name ,
         ' ' as emp_code ,
         ' ' as company_name ,
         0 as jour_amount ,
         0 as accomm_amount ,
         0 as subsidy_amount ,
         0 as other_amount ,
         0 as check_amount ,
         0 as jzpz ,
        '差旅费' as target_classify ,
         0 as meeting_amount ,
         exp_type_name ,
         ' ' as next_bill_id ,
         ' ' as last_bill_id ,
         ' ' as appr_org_sfname ,
         ' ' as sales_address ,
         ' ' as meet_addr ,
         ' ' as sponsor ,
         0 as jzpz_tax ,
         ' ' as billingdate ,
         ' ' as remarks ,
         0 as hotel_amount ,
         0 as total_amount ,
         ' ' as apply_id ,
         ' ' as base_apply_date ,
         ' ' as scenery_name_details ,
         ' ' as meet_num ,
         0 as diff_met_date ,
         0 as diff_met_date_avg ,
         ' ' as tb_times ,
         ' ' as receipt_city ,
         ' ' as commodityname ,
         ' ' as category_name,
         ' ' as iscompany,
         ' ' as origin_province,
         ' ' as destin_province,
        importdate
        FROM 01_datamart_layer_007_h_cw_df.finance_rma_travel_accomm
    WHERE {condition_sql}
        """.format(condition_sql=condition_sql).replace('\n', '').replace('\r', '').strip()

    print(sql)

    try:
        start_time = time.perf_counter()
        prod_execute_sql(conn_type='test', sqltype='insert', sql=sql)
        consumed_time = round(time.perf_counter() - start_time)
        print(f'*** 执行SQL耗时 {consumed_time} sec')
    except Exception as e:
        print(e)
        raise RuntimeError(e)


check_14_plane_data()  # 共有数据 3467564 条, 花费时间 3689 seconds
analyze_plane_data()
#kill_pid()
print('--- ok --')
