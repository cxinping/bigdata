# -*- coding: utf-8 -*-
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
import os
import pandas as pd
import time
import threading
from report.commons.connect_kudu import prod_execute_sql
from report.commons.logging import get_logger

log = get_logger(__name__)

import sys

sys.path.append('/you_filed_algos/app')

dest_dir = '/you_filed_algos/prod_kudu_data/checkpoint14'
dest_file = dest_dir + '/check_14_data.txt'


def check_14_data():
    """
    通过对比出发地与目的地相同或相近的报销项目，关注交通费偏离平均值或大多数人费用分布的较大情况。
    """
    columns_ls = ['bill_id', 'origin_name', 'destin_name', 'jour_amount']
    columns_str = ",".join(columns_ls)

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
    threadPool = ThreadPoolExecutor(max_workers=30)
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


def exec_task(sql):
    records = prod_execute_sql(conn_type='test', sqltype='select', sql=sql)
    if records and len(records) > 0:
        for idx, record in enumerate(records):
            bill_id = str(record[0])      # bill_id
            origin_name = str(record[1])  # 出发地
            destin_name = str(record[2])  # 目的地
            jour_amount = str(record[3])  # 交通费

            record_str = f'{bill_id},{origin_name},{destin_name},{jour_amount}'
            log.info(f" {threading.current_thread().name} is doing ")
            log.info(record_str)
            print()

            with open(dest_file, "a+", encoding='utf-8') as file:
                file.write(record_str + "\n")


def init_file():
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)

    if os.path.exists(dest_file):
        os.remove(dest_file)


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

def analyze_data_data():
    rd_df = pd.read_csv(dest_file, sep=',', header=None,
                        names=['bill_id', 'origin_name', 'destin_name', 'jour_amount'])
    # print(rd_df.dtypes)
    print('before filter ', len(rd_df))
    print(rd_df.head(20))
    print(len(rd_df))

def main():
    #init_file()
    #check_14_data()  # 3108210   1391728

    analyze_data_data()

    print('--- ok ---')
    os._exit(0)  # 无错误退出


main()
