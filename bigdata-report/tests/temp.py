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

    sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill WHERE plane_check_amount > 0 AND isPlane = 'plane' AND ( plane_origin_name is not null AND plane_destin_name is not null) limit 100001".format(
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
        tmp_sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill WHERE plane_check_amount > 0 AND isPlane = 'plane' AND ( plane_origin_name is not null AND plane_destin_name is not null)".format(
            columns_str=columns_str)
        select_sql_ls.append(tmp_sql)

    log.info('* 开始分页查询')

    # threadPool = ThreadPoolExecutor(max_workers=20, thread_name_prefix="thr")
    with ThreadPoolExecutor(max_workers=3, thread_name_prefix="thr") as threadPool:
        start_time = time.perf_counter()
        all_task = [threadPool.submit(exec_plane_task, sel_sql, plane_dest_file) for sel_sql in select_sql_ls]
        #wait(all_task, return_when=ALL_COMPLETED)

        done_futures, not_done_futures = wait(all_task, 0.2, return_when="ALL_COMPLETED")
        print(f"1 已完成：{done_futures}")
        print(f"2 未完成：{not_done_futures}")

        print('* waite threadPool completed')

        for future in as_completed(all_task):
            threadPool.shutdown()

    print('* 222 All tasks complete')
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* 查询耗时 {consumed_time} sec')
    kill_pid()


parent_pid = 0

import psutil
def kill_pid():
    parent = psutil.Process(parent_pid)
    for child in parent.children(recursive=True):  # or parent.children() for recursive=False
        child.kill()
    parent.kill()

def stop_process_pool(executor):
    for pid, process in executor._processes.items():
        process.terminate()
    executor.shutdown()


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


check_14_plane_data()  # 共有数据 3467564 条, 花费时间 3689 seconds
print('--- ok --')

print('1--- dis_connection --')

