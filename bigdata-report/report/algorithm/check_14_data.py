# -*- coding: utf-8 -*-
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
import os
import pandas as pd
import time

from report.commons.connect_kudu import prod_execute_sql
from report.commons.logging import get_logger

log = get_logger(__name__)

import sys

sys.path.append('/you_filed_algos/app')


def check_14_data():
    """
    通过对比出发地与目的地相同或相近的报销项目，关注交通费偏离平均值或大多数人费用分布的较大情况。
    """
    columns_ls = ['bill_id', 'origin_name', 'destin_name', 'jour_amount']
    columns_str = ",".join(columns_ls)

    # 44745309
    # 1745309 67
    # 2745309 118
    sql = 'select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill WHERE jour_amount > 0'.format(
        columns_str=columns_str)

    count_sql = 'select count(a.bill_id) from ({sql}) a'.format(sql=sql)
    log.info(count_sql)
    records = prod_execute_sql(conn_type='test', sqltype='select', sql=count_sql)
    count_records = records[0][0]

    max_size = 1 * 1000000
    limit_size = 1000
    select_sql_ls = []

    log.info(f'* count_records ==> {count_records}')
    if count_records >= max_size:
        offset_size = 0
        while offset_size <= count_records:
            if offset_size + limit_size > count_records:
                limit_size = count_records - offset_size
                tmp_sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill  WHERE jour_amount > 0 order by bill_id limit {limit_size} offset {offset_size}".format(
                    columns_str=columns_str, limit_size=limit_size, offset_size=offset_size)

                select_sql_ls.append(tmp_sql)
                break
            else:
                tmp_sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill  WHERE jour_amount > 0 order by bill_id limit {limit_size} offset {offset_size}".format(
                    columns_str=columns_str, limit_size=limit_size, offset_size=offset_size)
                select_sql_ls.append(tmp_sql)

            offset_size = offset_size + limit_size
    else:
        tmp_sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill".format(
            columns_str=columns_str)
        select_sql_ls.append(tmp_sql)

    log.info('* 开始分页查询')

    obj_list = []
    threadPool = ThreadPoolExecutor(max_workers=20)
    start_time = time.perf_counter()

    for sel_sql in select_sql_ls:
        log.info(sel_sql)
        obj = threadPool.submit(exec_task, sel_sql, columns_ls)
        obj_list.append(obj)

    # rd_list = []
    # for future in as_completed(obj_list):
    #     data = future.result()
    #     rd_list.append(data)
    #     print('* len(data)=', len(data))

    all_task = [threadPool.submit(exec_task, (sel_sql)) for sel_sql in select_sql_ls]
    wait(all_task, return_when=ALL_COMPLETED)

    threadPool.shutdown(wait=True)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* 查询耗时 {consumed_time} sec')


def exec_task(sql, columns_ls):
    pass


def calculate_data(rd_df):
    print(rd_df.head())
    print(rd_df.dtypes)
    print('*' * 50)
    print(rd_df.describe())

    temp = rd_df.describe()[['check_amount']]
    mean_val = temp.at['mean', 'check_amount']  # 平均值
    std_val = temp.at['std', 'check_amount']  # 方差

    result = rd_df[rd_df['check_amount'] > mean_val]
    print(result)

    bill_id_ls = result['bill_id'].tolist()
    exec_sql(bill_id_ls)


def exec_sql(bill_id_ls):
    print(len(bill_id_ls))


check_14_data()
print('--- ok ---')
os._exit(0)  # 无错误退出
