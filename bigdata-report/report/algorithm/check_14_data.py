# -*- coding: utf-8 -*-

from gevent import monkey;
monkey.patch_all(thread=False)

import gevent
from gevent.pool import Pool

from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
import os
import pandas as pd
import numpy as np
import time
import threading
from report.commons.connect_kudu2 import prod_execute_sql
from report.commons.db_helper import query_kudu_data
from report.commons.logging import get_logger
from report.commons.tools import list_of_groups
from report.services.common_services import query_billds_finance_all_targets
from report.commons.settings import CONN_TYPE

log = get_logger(__name__)

"""
https://blog.csdn.net/lzx159951/article/details/104357909

异常值：一组测定值中与平均值的偏差超过两倍标准差的测定值

cd /you_filed_algos/app

PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/algorithm/check_14_data.py


"""

import sys

sys.path.append('/you_filed_algos/app')

# 设置显示最大列数 与 显示宽度
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 500)

dest_dir = '/you_filed_algos/prod_kudu_data/checkpoint14'
no_plane_dest_file = dest_dir + '/check_14_no_plane_data.txt'
plane_dest_file = dest_dir + '/check_14_plane_data.txt'

test_limit_cond = ''  # 'LIMIT 1000'``


def check_14_plane_data():
    """
    是飞机的交通费
    通过对比出发地与目的地相同或相近的报销项目，关注交通费偏离平均值或大多数人费用分布的较大情况。

     SELECT finance_travel_id,bill_id,plane_beg_date, plane_end_date,plane_origin_name, plane_destin_name, plane_check_amount
     FROM 01_datamart_layer_007_h_cw_df.finance_travel_bill
     WHERE plane_check_amount > 0 AND isPlane = 'plane' AND ( plane_origin_name is not null AND plane_destin_name is not null)
     AND (plane_beg_date is not null AND plane_beg_date !='')

    """
    init_file(plane_dest_file)

    columns_ls = ['finance_travel_id', 'bill_id', 'plane_beg_date', 'plane_end_date', 'plane_origin_name',
                  'plane_destin_name', 'plane_check_amount']
    columns_str = ",".join(columns_ls)

    sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill WHERE plane_check_amount > 0 AND isPlane = 'plane' AND ( plane_origin_name is not null AND plane_destin_name is not null) AND (plane_beg_date is not null AND plane_beg_date !='') {test_limit_cond} ".format(
        columns_str=columns_str, test_limit_cond=test_limit_cond)

    count_sql = 'select count(a.finance_travel_id) from ({sql}) a'.format(sql=sql)
    log.info(count_sql)
    records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=count_sql)
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
        tmp_sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill WHERE plane_check_amount > 0 AND isPlane = 'plane' AND ( plane_origin_name is not null AND plane_destin_name is not null) AND (plane_beg_date is not null AND plane_beg_date !='') {test_limit_cond}".format(
            columns_str=columns_str, test_limit_cond=test_limit_cond)
        select_sql_ls.append(tmp_sql)

    log.info('* check_14_plane_data 开始分页查询')

    start_time = time.perf_counter()

    # threadPool = ThreadPoolExecutor(max_workers=10, thread_name_prefix="thr")
    # all_task = [threadPool.submit(exec_plane_task, sel_sql, plane_dest_file) for sel_sql in select_sql_ls]
    # wait(all_task, return_when=ALL_COMPLETED)
    # threadPool.shutdown(wait=True)

    pool = Pool(30)
    results = []
    for sel_sql in select_sql_ls:
        rst = pool.spawn(exec_plane_task, sel_sql, plane_dest_file)
        results.append(rst)
    gevent.joinall(results)

    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* check_14_plane_data 一共有数据 {count_records} 条,保存数据耗时 {consumed_time} sec')


def exec_plane_task(sql, dest_file):  # dest_file
    records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sql)
    # time.sleep(0.01)

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

            # log.info(f"checkpoint14 plane {threading.current_thread().name} is running")
            # log.info(record_str)
            # print()

            with open(dest_file, "a+", encoding='utf-8') as file:
                file.write(record_str + "\n")


def check_14_no_plane_data():
    """
    非飞机的交通费
    通过对比出发地与目的地相同或相近的报销项目，关注交通费偏离平均值或大多数人费用分布的较大情况。

    select finance_travel_id, bill_id, origin_name, destin_name, jour_amount
    from 01_datamart_layer_007_h_cw_df.finance_travel_bill WHERE jour_amount > 0 AND isPlane is NULL AND (origin_name is not NULL AND destin_name is not NULL)
    """
    init_file(no_plane_dest_file)

    columns_ls = ['finance_travel_id', 'bill_id', 'origin_name', 'destin_name', 'jour_amount']
    columns_str = ",".join(columns_ls)

    sql = 'select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill WHERE jour_amount > 0 AND isPlane is NULL AND (origin_name is not NULL AND destin_name is not NULL) {test_limit_cond}'.format(
        columns_str=columns_str, test_limit_cond=test_limit_cond)

    count_sql = 'select count(a.finance_travel_id) from ({sql}) a'.format(sql=sql)
    log.info(count_sql)
    records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=count_sql)
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
                tmp_sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill  WHERE jour_amount > 0 AND isPlane is NULL  AND (origin_name is not NULL AND destin_name is not NULL)  ORDER BY finance_travel_id limit {limit_size} offset {offset_size}".format(
                    columns_str=columns_str, limit_size=limit_size, offset_size=offset_size)

                select_sql_ls.append(tmp_sql)
                break
            else:
                tmp_sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill  WHERE jour_amount > 0 AND isPlane is NULL  AND (origin_name is not NULL AND destin_name is not NULL)  ORDER BY finance_travel_id limit {limit_size} offset {offset_size}".format(
                    columns_str=columns_str, limit_size=limit_size, offset_size=offset_size)
                select_sql_ls.append(tmp_sql)

            offset_size = offset_size + limit_size
    else:
        tmp_sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill  WHERE jour_amount > 0 AND isPlane is NULL AND (origin_name is not NULL AND destin_name is not NULL)  {test_limit_cond} ".format(
            columns_str=columns_str, test_limit_cond=test_limit_cond)
        select_sql_ls.append(tmp_sql)

    log.info('* check_14_no_plane_data 开始分页查询')

    start_time = time.perf_counter()

    # threadPool = ThreadPoolExecutor(max_workers=10, thread_name_prefix="thr")
    # all_task = [threadPool.submit(exec_no_plane_task, sel_sql, no_plane_dest_file) for sel_sql in select_sql_ls]
    # wait(all_task, return_when=ALL_COMPLETED)
    # threadPool.shutdown(wait=True)

    pool = Pool(30)
    results = []
    for sel_sql in select_sql_ls:
        rst = pool.spawn(exec_no_plane_task, sel_sql, no_plane_dest_file)
        results.append(rst)
    gevent.joinall(results)

    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* check_14_no_plane_data 一共有数据 {count_records} 条, 保存数据耗时 {consumed_time} sec')


def exec_no_plane_task(sql, dest_file):
    records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sql)

    if records and len(records) > 0:
        for idx, record in enumerate(records):
            finance_travel_id = str(record[0])  # finance_travel_id
            bill_id = str(record[1])  # bill_id
            origin_name = str(record[2])  # 出发地
            destin_name = str(record[3])  # 目的地
            jour_amount = str(record[4])  # 交通费

            origin_name = origin_name.replace(',', ' ')
            destin_name = destin_name.replace(',', ' ')

            record_str = f'{finance_travel_id},{bill_id},{origin_name},{destin_name},{jour_amount}'

            # log.info(f"checkpoint14 no_plane {threading.current_thread().name} is running")
            # log.info(record_str)
            # print()

            with open(dest_file, "a+", encoding='utf-8') as file:
                file.write(record_str + "\n")


def init_file(dest_file):
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)

    if os.path.exists(dest_file):
        os.remove(dest_file)

    os.mknod(dest_file)


def analyze_no_plane_data(coefficient=2):
    """

    排除飞机票后, 取其他所有的差旅费

    :param coefficient: 系数，默认值为2
    :return:
    """

    log.info(f'========== check_14 analyze_no_plane_data coefficient={coefficient} ')
    start_time = time.perf_counter()

    rd_df = pd.read_csv(no_plane_dest_file, sep=',', header=None,
                        # dtype={'finance_travel_id': str, 'origin_name' : str, 'destin_name' : str, 'jour_amount': np.float64},
                        dtype={'finance_travel_id': str, 'bill_id': str, 'origin_name': str, 'destin_name': str,
                               'jour_amount': np.float64},
                        encoding="utf-8",
                        names=['finance_travel_id', 'bill_id', 'origin_name', 'destin_name', 'jour_amount'])

    # print(rd_df.dtypes)
    # print(rd_df.head(20))
    # print(len(rd_df))
    # print('*' * 50)
    # test
    # rd_df = rd_df[:500]
    # print(rd_df.head(20))
    # print(len(rd_df))
    # print('=' * 50)

    grouped_df = rd_df.groupby(['origin_name', 'destin_name'])

    abnormal_bill_id_ls = []
    for name, group_df in grouped_df:
        origin_name, destin_name = name
        # temp = group_df.describe()[['jour_amount']]
        # std_val = temp.at['std', 'jour_amount']  # 标准差
        # mean_val = temp.at['mean', 'jour_amount']  # 平均值

        std_val = group_df.std().at['jour_amount']  # 标准差
        mean_val = group_df.mean().at['jour_amount']  # 平均值

        if std_val == 0 or np.isnan(std_val):
            std_val = 0

        # 数据的正常范围为 【mean - 2 × std , mean + 2 × std】
        max_val = mean_val + coefficient * std_val
        min_val = mean_val - coefficient * std_val

        if len(group_df) >= 2:
            # print(f'origin_name={origin_name}, destin_name={destin_name}, 每组数据的数量={len(group_df)}, coefficient系数为 {coefficient}, 标准差={std_val},平均值={mean_val}, 数据的正常范围为 {min_val} 到 {max_val}')
            # print(group_df)
            # print('')

            for index, row in group_df.iterrows():
                jour_amount = row['jour_amount']
                # print('jour_amount=', jour_amount)

                if jour_amount > max_val: # jour_amount > max_val or jour_amount < min_val:
                    bill_id = row['bill_id']
                    abnormal_bill_id_ls.append(bill_id)
            # print('')

    # print('----  show result ----')
    # print(abnormal_bill_id_ls)
    del grouped_df
    del rd_df

    targes_bill_id_ls = query_billds_finance_all_targets(unusual_id='14')
    abnormal_bill_id_ls = [x for x in abnormal_bill_id_ls if x not in targes_bill_id_ls]

    # print(len(abnormal_bill_id_ls))

    exec_no_plane_sql(abnormal_bill_id_ls)

    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* 执行检查点14 no_plane 的数据共耗时 {consumed_time} sec')


def exec_plane_sql(bill_id_ls):
    print('exec_plane_sql ==> ', len(bill_id_ls))

    if bill_id_ls and len(bill_id_ls) > 0:
        group_ls = list_of_groups(bill_id_ls, 1000)
        # print(len(group_ls), group_ls)

        condition_sql = ''
        in_codition = 'bill_id IN {temp}'

        for idx, group in enumerate(group_ls):
            if len(group) == 1:
                temp = in_codition.format(temp=str('("' + group[0] + '")'))
            else:
                temp = in_codition.format(temp=str(tuple(group)))

            if idx == 0:
                condition_sql = temp
            else:
                condition_sql = condition_sql + ' OR ' + temp

        # print(condition_sql)

        sql = """
        UPSERT INTO analytic_layer_zbyy_cwyy_014_cwzbbg.finance_all_targets    
            SELECT
            finance_travel_id as finance_id,
            bill_id,
            '14' as unusual_id,
            company_code,
            account_period,
            finance_number,
            profit_center,
            cart_head,
            bill_code,
            bill_beg_date,
            bill_end_date,
            '' as origin_city,
            '' as destin_city,
            '' as beg_date,
            '' as end_date,
            apply_emp_name,
            '' as emp_name,
            '' as emp_code,
            '' as company_name,
            jour_amount,
            accomm_amount,
            subsidy_amount,
            other_amount,
            check_amount,
            jzpz,
            '差旅费' as target_classify,
            0 as meeting_amount,
            '' as exp_type_name,
            '' as next_bill_id,
            '' as last_bill_id,
            appr_org_sfname,
            sales_address,
            '' as meet_addr,
            '' as sponsor,
            jzpz_tax,
            billingdate,
            '' as remarks,
            0 as hotel_amount,
            0 as total_amount,
            apply_id,
            base_apply_date,
            '' as scenery_name_details,
            '' as meet_num,
            0 as diff_met_date,
            0 as diff_met_date_avg,
            tb_times,
            receipt_city,
            commodityname,
            '' as category_name,
            iscompany,
            origin_province,
            destin_province,
            operation_time,
            doc_date,
            operation_emp_name,
            invoice_type_name,
            taxt_amount,
            original_tax_amount,
            js_times,
            '' as offset_day,
            '' as meet_lvl_name,
            '' as meet_type_name,
            0 as buget_limit,
            0 as sum_person,
            invo_number,
            invo_code,
            '' as city,
            0 as amounttax,
            '' as offset_ratio,
            '' as amounttax_ratio,
            '' as ratio,
            '' as approve_name,
            importdate
            FROM 01_datamart_layer_007_h_cw_df.finance_travel_bill
        WHERE {condition_sql}
            """.format(condition_sql=condition_sql)  # .replace('\n', '').replace('\r', '').strip()

        # print(sql)

        try:
            start_time = time.perf_counter()
            prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)
            consumed_time = round(time.perf_counter() - start_time)
            print(f'*** 执行 plane SQL耗时 {consumed_time} sec')
        except Exception as e:
            print(e)
            raise RuntimeError(e)


def exec_no_plane_sql(bill_id_ls):
    print('exec_no_plane_sql ==> ', len(bill_id_ls))

    if bill_id_ls and len(bill_id_ls) > 0:
        group_ls = list_of_groups(bill_id_ls, 1000)
        # print(len(group_ls), group_ls)

        condition_sql = ''
        in_codition = 'bill_id IN {temp}'

        for idx, group in enumerate(group_ls):
            if len(group) == 1:
                temp = in_codition.format(temp=str('("' + group[0] + '")'))
            else:
                temp = in_codition.format(temp=str(tuple(group)))

            if idx == 0:
                condition_sql = temp
            else:
                condition_sql = condition_sql + ' OR ' + temp

        # print(condition_sql)

        sql = """
        UPSERT INTO analytic_layer_zbyy_cwyy_014_cwzbbg.finance_all_targets    
        SELECT
        finance_travel_id as finance_id,
        bill_id,
        '14' as unusual_id,
        company_code,
        account_period,
        finance_number,
        profit_center,
        cart_head,
        bill_code,
        bill_beg_date,
        bill_end_date,
        '' as origin_city,
        '' as destin_city,
        '' as beg_date,
        '' as end_date,
        apply_emp_name,
        '' as emp_name,
        '' as emp_code,
        '' as company_name,
        jour_amount,
        accomm_amount,
        subsidy_amount,
        other_amount,
        check_amount,
        jzpz,
        '差旅费' as target_classify,
        0 as meeting_amount,
        '' as exp_type_name,
        '' as next_bill_id,
        '' as last_bill_id,
        appr_org_sfname,
        sales_address,
        '' as meet_addr,
        '' as sponsor,
        jzpz_tax,
        billingdate,
        '' as remarks,
        0 as hotel_amount,
        0 as total_amount,
        apply_id,
        base_apply_date,
        '' as scenery_name_details,
        '' as meet_num,
        0 as diff_met_date,
        0 as diff_met_date_avg,
        tb_times,
        receipt_city,
        commodityname,
        '' as category_name,
        iscompany,
        origin_province,
        destin_province,
        operation_time,
        doc_date,
        operation_emp_name,
        invoice_type_name,
        taxt_amount,
        original_tax_amount,
        js_times,
        '' as offset_day,
        '' as meet_lvl_name,
        '' as meet_type_name,
        0 as buget_limit,
        0 as sum_person,
        invo_number,
        invo_code,
        '' as city,
        0 as amounttax,
        '' as offset_ratio,
        '' as amounttax_ratio,
        '' as ratio,
        '' as approve_name,
        importdate
            FROM 01_datamart_layer_007_h_cw_df.finance_travel_bill 
        WHERE {condition_sql}
            """.format(condition_sql=condition_sql)  # .replace('\n', '').replace('\r', '').strip()

        # print(sql)

        try:
            start_time = time.perf_counter()
            prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)
            consumed_time = round(time.perf_counter() - start_time)
            print(f'*** 执行 no plane SQL耗时 {consumed_time} sec')
        except Exception as e:
            print(e)
            raise RuntimeError(e)


def analyze_plane_data(coefficient=2):
    """
    只包括飞机票费用

    :param coefficient: 系数，默认值为2
    :return:
    """

    log.info(f'======= check_14 analyze_plane_data coefficient={coefficient} ')
    start_time = time.perf_counter()

    rd_df = pd.read_csv(plane_dest_file, sep=',', header=None, encoding="utf-8",
                        dtype={'finance_travel_id': str, 'bill_id': str, 'plane_beg_date': str, 'plane_end_date': str,
                               'plane_origin_name': str, 'plane_destin_name': str, 'jour_amount': np.float64},
                        names=['finance_travel_id', 'bill_id', 'plane_beg_date', 'plane_end_date', 'plane_origin_name',
                               'plane_destin_name', 'plane_check_amount'])

    # print(rd_df.dtypes)
    # print('* counts => ', len(rd_df))
    # test
    # rd_df = rd_df[:1500]
    # print(rd_df.head(10))

    grouped_df = rd_df.groupby(['plane_beg_date', 'plane_origin_name', 'plane_destin_name'], as_index=False, sort=False)
    # grouped_df = rd_df.groupby([ 'plane_origin_name', 'plane_destin_name'])
    # print('* after groupby ')

    bill_id_ls = []
    for name, group_df in grouped_df:
        # print(name)
        plane_beg_date, plane_origin_name, plane_destin_name = name
        # temp = group_df.describe()[['plane_check_amount']]
        # std_val = temp.at['std', 'plane_check_amount']  # 标准差
        # mean_val = temp.at['mean', 'plane_check_amount']  # 平均值

        std_val = group_df.std().at['plane_check_amount']  # 标准差
        mean_val = group_df.mean().at['plane_check_amount']  # 平均值

        if std_val == 0 or np.isnan(std_val):
            std_val = 0

        # 数据的正常范围为 【mean - 2 × std , mean + 2 × std】
        max_val = mean_val + coefficient * std_val
        min_val = mean_val - coefficient * std_val

        if len(group_df) >= 2:
            str_val = f'plane_beg_date={plane_beg_date}, plane_origin_name={plane_origin_name}, plane_destin_name={plane_destin_name}, 每组数据的数量={len(group_df)}, coefficient系数为 {coefficient}, 标准差={std_val},平均值={mean_val}, 数据的正常范围为 {min_val} 到 {max_val}'
            # print(str_val)
            # print(group_df)
            # print('')

            for index, row in group_df.iterrows():
                plane_check_amount = row['plane_check_amount']
                if plane_check_amount > max_val: # plane_check_amount > max_val or plane_check_amount < min_val:
                    bill_id = row['bill_id']
                    bill_id_ls.append(bill_id)

                    # print(row)
                    # print()

    # print('---- show result ---')
    # print(bill_id_ls)

    del grouped_df
    del rd_df

    targes_bill_id_ls = query_billds_finance_all_targets(unusual_id='14')
    bill_id_ls = [x for x in bill_id_ls if x not in targes_bill_id_ls]

    exec_plane_sql(bill_id_ls)

    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* 执行检查点14 plane 的数据共耗时 {consumed_time} sec')


def check_14_plane_data2():
    columns_ls = ['finance_travel_id', 'bill_id', 'plane_beg_date', 'plane_end_date', 'plane_origin_name',
                  'plane_destin_name',
                  'plane_check_amount']
    columns_str = ",".join(columns_ls)

    sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill WHERE plane_check_amount > 0 AND isPlane = 'plane' AND ( plane_origin_name is not null AND plane_destin_name is not null)".format(
        columns_str=columns_str)

    count_sql = 'select count(a.finance_travel_id) from ({sql}) a'.format(sql=sql)
    log.info(count_sql)
    records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=count_sql)
    count_records = records[0][0]

    log.info(f'* count_records ==> {count_records}')

    max_size = 1 * 100000
    limit_size = 1 * 10000
    select_sql_ls = []

    if count_records >= max_size:
        offset_size = 0
        while offset_size <= count_records:
            if offset_size + limit_size > count_records:
                limit_size = count_records - offset_size
                tmp_sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill WHERE plane_check_amount > 0 AND isPlane = 'plane' AND ( plane_origin_name is not null AND plane_destin_name is not null) ORDER BY plane_beg_date limit {limit_size} offset {offset_size}".format(
                    columns_str=columns_str, limit_size=limit_size, offset_size=offset_size)

                select_sql_ls.append(tmp_sql)
                break
            else:
                tmp_sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill WHERE plane_check_amount > 0 AND isPlane = 'plane' AND ( plane_origin_name is not null AND plane_destin_name is not null) ORDER BY plane_beg_date limit {limit_size} offset {offset_size}".format(
                    columns_str=columns_str, limit_size=limit_size, offset_size=offset_size)
                select_sql_ls.append(tmp_sql)

            offset_size = offset_size + limit_size
    else:
        tmp_sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill WHERE plane_check_amount > 0 AND isPlane = 'plane' AND ( plane_origin_name is not null AND plane_destin_name is not null) ".format(
            columns_str=columns_str)
        select_sql_ls.append(tmp_sql)

    log.info(f'* 开始分页合并dataframe,一共 {len(select_sql_ls)} 页')

    rt_df = None  # count 3467564 , page 347
    start_time = time.perf_counter()
    for idx, sel_sql in enumerate(select_sql_ls):
        # print(idx, sel_sql)

        if idx == 0:
            rt_df = query_kudu_data(sql=sel_sql, columns=columns_ls, conn_type=CONN_TYPE)
        else:
            tmp_df = query_kudu_data(sql=sel_sql, columns=columns_ls, conn_type=CONN_TYPE)
            rt_df = rt_df.append(tmp_df, ignore_index=True)

    consumed_time = round(time.perf_counter() - start_time)
    print(f'consumed_time={consumed_time}')
    print(len(rt_df))


def task1(coefficient):
    # 需求1 交通方式为非飞机的交通费用异常分析
    start_time = time.perf_counter()
    check_14_no_plane_data()  # 一共有数据 6428955 条, 保存数据耗时 4389 sec
    analyze_no_plane_data(coefficient=coefficient)  # task1 任务耗时 4752 sec

    consumed_time = round(time.perf_counter() - start_time)
    print(f'****** task1 任务耗时 {consumed_time} sec')
    print('--- analyze_no_plane_data has been completed ---')


def task2(coefficient):
    # 需求2 交通方式为飞机的交通费用异常分析
    start_time = time.perf_counter()
    check_14_plane_data()  # 一共有数据 6352119 条,保存数据耗时 4774 sec
    analyze_plane_data(coefficient=coefficient)  # task2 任务耗时 19293 sec

    consumed_time = round(time.perf_counter() - start_time)
    print(f'****** task2 任务耗时 {consumed_time} sec')
    print('--- analyze_plane_data has been completed ---')

    # check_14_plane_data2()


def main():
    """
    执行检查点14 plane 的数据共耗时 15683 sec

    """

    with ThreadPoolExecutor(max_workers=2) as ex:
        print('main: starting')
        ex.submit(task1, 4)
        ex.submit(task2, 4)

    print('*** main: done ***')


main()
