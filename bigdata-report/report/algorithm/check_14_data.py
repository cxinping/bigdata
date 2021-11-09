# -*- coding: utf-8 -*-
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
import os
import pandas as pd
import numpy as np
import time
import threading
from report.commons.connect_kudu import prod_execute_sql
from report.commons.logging import get_logger

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


def check_14_plane_data():
    """
    是飞机的交通费
    通过对比出发地与目的地相同或相近的报销项目，关注交通费偏离平均值或大多数人费用分布的较大情况。

     select finance_travel_id,plane_beg_date, plane_end_date,plane_origin_name, plane_destin_name, plane_check_amount from 01_datamart_layer_007_h_cw_df.finance_travel_bill
     WHERE plane_check_amount > 0 AND isPlane = 'plane' AND ( plane_origin_name is not null AND plane_destin_name is not null)

    """
    init_file(plane_dest_file)

    columns_ls = ['finance_travel_id', 'plane_beg_date', 'plane_end_date', 'plane_origin_name', 'plane_destin_name',
                  'plane_check_amount']
    columns_str = ",".join(columns_ls)

    sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill WHERE plane_check_amount > 0 AND isPlane = 'plane' AND ( plane_origin_name is not null AND plane_destin_name is not null)".format(
        columns_str=columns_str)

    count_sql = 'select count(a.finance_travel_id) from ({sql}) a'.format(sql=sql)
    log.info(count_sql)
    records = prod_execute_sql(conn_type='test', sqltype='select', sql=count_sql)
    count_records = records[0][0]

    max_size = 1 * 100000
    limit_size = 10000
    select_sql_ls = []

    log.info(f'* count_records ==> {count_records}')
    if count_records >= max_size:
        offset_size = 0
        while offset_size <= count_records:
            if offset_size + limit_size > count_records:
                limit_size = count_records - offset_size
                tmp_sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill WHERE plane_check_amount > 0 AND isPlane = 'plane' AND ( plane_origin_name is not null AND plane_destin_name is not null) ORDER BY finance_travel_id limit {limit_size} offset {offset_size}".format(
                    columns_str=columns_str, limit_size=limit_size, offset_size=offset_size)

                select_sql_ls.append(tmp_sql)
                break
            else:
                tmp_sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill WHERE plane_check_amount > 0 AND isPlane = 'plane' AND ( plane_origin_name is not null AND plane_destin_name is not null) ORDER BY finance_travel_id limit {limit_size} offset {offset_size}".format(
                    columns_str=columns_str, limit_size=limit_size, offset_size=offset_size)
                select_sql_ls.append(tmp_sql)

            offset_size = offset_size + limit_size
    else:
        tmp_sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill WHERE plane_check_amount > 0 AND isPlane = 'plane' AND ( plane_origin_name is not null AND plane_destin_name is not null) ".format(
            columns_str=columns_str)
        select_sql_ls.append(tmp_sql)

    log.info('* 开始分页查询')

    threadPool = ThreadPoolExecutor(max_workers=30)
    start_time = time.perf_counter()

    all_task = [threadPool.submit(exec_task, sel_sql, plane_dest_file) for sel_sql in select_sql_ls]
    wait(all_task, return_when=ALL_COMPLETED)

    threadPool.shutdown(wait=True)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* 查询耗时 {consumed_time} sec')


def check_14_no_plane_data():
    """
    非飞机的交通费
    通过对比出发地与目的地相同或相近的报销项目，关注交通费偏离平均值或大多数人费用分布的较大情况。

    select finance_travel_id, origin_name, destin_name, jour_amount from 01_datamart_layer_007_h_cw_df.finance_travel_bill WHERE jour_amount > 0 AND isPlane is NULL AND (origin_name is not NULL AND destin_name is not NULL)
    """
    init_file(no_plane_dest_file)

    columns_ls = ['finance_travel_id', 'origin_name', 'destin_name', 'jour_amount']
    columns_str = ",".join(columns_ls)

    sql = 'select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill WHERE jour_amount > 0 AND isPlane is NULL AND (origin_name is not NULL AND destin_name is not NULL) '.format(
        columns_str=columns_str)

    count_sql = 'select count(a.finance_travel_id) from ({sql}) a'.format(sql=sql)
    log.info(count_sql)
    records = prod_execute_sql(conn_type='test', sqltype='select', sql=count_sql)
    count_records = records[0][0]

    max_size = 1 * 100000
    limit_size = 10000
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
        tmp_sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill  WHERE jour_amount > 0 AND isPlane is NULL AND (origin_name is not NULL AND destin_name is not NULL)  ".format(
            columns_str=columns_str)
        select_sql_ls.append(tmp_sql)

    log.info('* 开始分页查询')

    obj_list = []
    threadPool = ThreadPoolExecutor(max_workers=30)
    start_time = time.perf_counter()

    # for sel_sql in select_sql_ls:
    #     log.info(sel_sql)
    #     obj = threadPool.submit(exec_task, sel_sql, no_plane_dest_file)
    #     obj_list.append(obj)

    # rd_list = []
    # for future in as_completed(obj_list):
    #     data = future.result()
    #     rd_list.append(data)
    #     print('* len(data)=', len(data))

    print(select_sql_ls)

    all_task = [threadPool.submit(exec_task, sel_sql, no_plane_dest_file) for sel_sql in select_sql_ls]
    wait(all_task, return_when=ALL_COMPLETED)

    threadPool.shutdown(wait=True)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* 查询耗时 {consumed_time} sec')


def exec_task(sql, dest_file):  # dest_file
    records = prod_execute_sql(conn_type='test', sqltype='select', sql=sql)
    if records and len(records) > 0:
        for idx, record in enumerate(records):
            bill_id = str(record[0])  # bill_id
            origin_name = str(record[1])  # 出发地
            destin_name = str(record[2])  # 目的地
            jour_amount = str(record[3])  # 交通费

            record_str = f'{bill_id},{origin_name},{destin_name},{jour_amount}'
            # log.info(f'dest_file = {dest_file}')
            log.info(f" {threading.current_thread().name} is doing")
            log.info(record_str)
            print()

            with open(dest_file, "a+", encoding='utf-8') as file:
                file.write(record_str + "\n")


def init_file(dest_file):
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)

    if os.path.exists(dest_file):
        os.remove(dest_file)


def analyze_no_plane_data_data(coefficient=2):
    """

    排除飞机票后, 取其他所有的差旅费

    :param coefficient: 系数，默认值为2
    :return:
    """
    rd_df = pd.read_csv(no_plane_dest_file, sep=',', header=None,
                        names=['finance_travel_id', 'origin_name', 'destin_name', 'jour_amount'])
    # print(rd_df.dtypes)
    print('before filter ', len(rd_df))
    print(rd_df.head(20))
    print(len(rd_df))

    print('*' * 50)

    rd_df = rd_df.dropna(axis=0, how='any',subset=['origin_name', 'destin_name'])

    rd_df = rd_df[:300]
    print(rd_df)
    print(len(rd_df))

    # grouped_df = rd_df.groupby(['origin_name', 'destin_name'])
    #
    # for name, group_df in grouped_df:
    #     origin_name, destin_name = name
    #     temp = group_df.describe()[['jour_amount']]
    #     std_val = temp.at['std', 'jour_amount']    # 标准差
    #     mean_val = temp.at['mean', 'jour_amount']  # 平均值
    #
    #     if std_val == 0 or np.isnan(std_val):
    #         std_val = 0
    #
    #     # 数据的正常范围为 【mean - 2 × std , mean + 2 × std】
    #     max_val = mean_val + coefficient * std_val
    #     min_val = mean_val - coefficient * std_val
    #
    #     print(f'origin_name={origin_name}, destin_name={destin_name}, 每组数据的数量={len(group_df)}，标准差={std_val},平均值={mean_val}, 数据的正常范围为 {min_val} 到 {max_val}')
    #     print(group_df)
    #     print('')


def analyze_plane_data_data(coefficient=2):
    """
    只包括飞机票费用

    :param coefficient: 系数，默认值为2
    :return:
    """
    rd_df = pd.read_csv(plane_dest_file, sep=',', header=None,
                        names= ['finance_travel_id', 'plane_beg_date', 'plane_end_date', 'plane_origin_name', 'plane_destin_name',
                  'plane_check_amount'])

    rd_df = rd_df[:300]
    print(rd_df)
    grouped_df = rd_df.groupby(['plane_beg_date', 'plane_end_date','plane_origin_name', 'plane_destin_name'])

    for name, group_df in grouped_df:
        print(name)

        # origin_name, destin_name = name
        # temp = group_df.describe()[['plane_check_amount']]
        # std_val = temp.at['std', 'plane_check_amount']    # 标准差
        # mean_val = temp.at['mean', 'plane_check_amount']  # 平均值
        #
        # if std_val == 0 or np.isnan(std_val):
        #     std_val = 0
        #
        # # 数据的正常范围为 【mean - 2 × std , mean + 2 × std】
        # max_val = mean_val + coefficient * std_val
        # min_val = mean_val - coefficient * std_val

        # str_val = f'plane_beg_date={plane_beg_date}, destin_name={destin_name}, 每组数据的数量={len(group_df)}，标准差={std_val},平均值={mean_val}, 数据的正常范围为 {min_val} 到 {max_val}'
        # print(str_val )
        # print(group_df)
        # print('')


def main():
    check_14_no_plane_data()  # 4205254
    #analyze_no_plane_data_data(coefficient=2)

    #check_14_plane_data()  # 3493517  3493517
    #analyze_plane_data_data(coefficient=2)

    print('--- ok ---')
    os._exit(0)  # 无错误退出


main()
