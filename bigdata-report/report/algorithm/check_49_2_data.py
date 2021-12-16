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
cd /you_filed_algos/app

PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/algorithm/check_49_2_data.py


"""

import sys

sys.path.append('/you_filed_algos/app')

import pandas as pd

# 设置显示最大列数 与 显示宽度
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 500)

dest_dir = '/you_filed_algos/prod_kudu_data/checkpoint49'
dest_file = dest_dir + '/check_49_data.txt'

test_limit_cond = ''  # 'LIMIT 1000'``


def init_file(dest_file):
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)

    if os.path.exists(dest_file):
        os.remove(dest_file)

    os.mknod(dest_file)


def save_data():
    init_file(dest_file)

    columns_ls = ['finance_offical_id', 'bill_id', 'bill_code', 'check_amount']  # 日期字段 account_period
    columns_str = ",".join(columns_ls)

    sql = 'select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_official_bill where check_amount > 0 AND bill_code is not NULL AND bill_code !=""  '.format(
        columns_str=columns_str)

    count_sql = 'select count(a.bill_id) from ({sql}) a'.format(sql=sql)
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
                tmp_sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_official_bill where check_amount > 0 AND bill_code is not NULL AND bill_code !='' ORDER BY check_amount limit {limit_size} offset {offset_size}".format(
                    columns_str=columns_str, limit_size=limit_size, offset_size=offset_size)

                select_sql_ls.append(tmp_sql)
                break
            else:
                tmp_sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_official_bill where check_amount > 0 AND bill_code is not NULL AND bill_code !='' ORDER BY check_amount limit {limit_size} offset {offset_size}".format(
                    columns_str=columns_str, limit_size=limit_size, offset_size=offset_size)
                select_sql_ls.append(tmp_sql)

            offset_size = offset_size + limit_size
    else:
        tmp_sql = "select {columns_str} 01_datamart_layer_007_h_cw_df.finance_official_bill where check_amount > 0 AND bill_code is not NULL AND bill_code !='' ".format(
            columns_str=columns_str, test_limit_cond=test_limit_cond)
        select_sql_ls.append(tmp_sql)

    log.info('* check_49_plane_data 开始分页查询')

    start_time = time.perf_counter()
    pool = Pool(30)
    results = []
    for sel_sql in select_sql_ls:
        rst = pool.spawn(exec_task, sel_sql)
        results.append(rst)
    gevent.joinall(results)

    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* check_49 一共有数据 {count_records} 条,保存数据耗时 {consumed_time} sec')


def exec_task(sql):
    records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sql)

    if records and len(records) > 0:
        for idx, record in enumerate(records):
            finance_offical_id = str(record[0])
            bill_id = str(record[1])
            bill_code = str(record[2])
            check_amount = str(record[3])

            record_str = f'{finance_offical_id},{bill_id},{bill_code},{check_amount}'

            with open(dest_file, "a+", encoding='utf-8') as file:
                file.write(record_str + "\n")


def analyze_data():
    log.info(f'========== check_49 analyze_data ===========')
    start_time = time.perf_counter()

    rd_df = pd.read_csv(dest_file, sep=',', header=None,
                        dtype={'finance_offical_id': str, 'bill_id': str, 'bill_code': str, 'check_amount': np.float64},
                        encoding="utf-8",
                        names=['finance_offical_id', 'bill_id', 'bill_code', 'check_amount'])
    #print(rd_df.dtypes)
    # mean_val = rd_df.mean().at['check_amount']  # 平均值
    std_val = rd_df.std().at['check_amount']  # 标准方差

    result = rd_df[rd_df['check_amount'] > std_val]
    print(f'* 计算的方差为 => {std_val}')
    #print(result.head(5))

    finance_id_ls = result['finance_offical_id'].tolist()


    if len(finance_id_ls) > 0:
        exec_sql(finance_id_ls)
    else:
        print('** finance_id_ls length is 0 ')

    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* check_49_2 分析数据耗时 {consumed_time} sec')


def exec_sql(finance_id_ls):
    print('* exec_sql ==> ', len(finance_id_ls))

    if finance_id_ls and len(finance_id_ls) > 0:
        group_ls = list_of_groups(finance_id_ls, 1000)
        # print(len(group_ls), group_ls)

        condition_sql = ''
        in_codition = 'finance_offical_id IN {temp}'

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
        finance_offical_id as finance_id,
        bill_id,
        '49' as unusual_id,
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
        company_name,
        0 as jour_amount,
        0 as accomm_amount,
        0 as subsidy_amount,
        0 as other_amount,
        check_amount,
        jzpz,
        '办公费' as target_classify,
        0 as meeting_amount,
        exp_type_name,
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
        '' as origin_province,
        '' as destin_province,
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
        from 01_datamart_layer_007_h_cw_df.finance_official_bill
        WHERE {condition_sql}
            """.format(condition_sql=condition_sql)

        # print(sql)

        try:
            start_time = time.perf_counter()
            prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)
            consumed_time = round(time.perf_counter() - start_time)
            print(f'*** 执行SQL耗时 {consumed_time} sec')
        except Exception as e:
            print(e)
            raise RuntimeError(e)


def main():
    save_data()  # 一共有数据 1540029 条,保存数据耗时 487 sec
    analyze_data()  # 分析数据耗时 147 sec


main()
