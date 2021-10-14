# -*- coding: utf-8 -*-
from report.commons.logging import get_logger
from report.commons.connect_kudu import prod_execute_sql, dis_connection
import time
import os
import json
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from report.commons.tools import match_address
from report.commons.test_hdfs_tools import HDFSTools as Test_HDFSTools

"""

把上传的数据放到 02_logical_layer_007_h_lf_cw.finance_travel_linshi_analysis 表里
"""

log = get_logger(__name__)

dest_file = "/you_filed_algos/prod_kudu_data/check_02_trip_data.txt"
upload_hdfs_path = '/user/hive/warehouse/02_logical_layer_007_h_lf_cw.db/finance_travel_linshi_analysis/'


def init_file():
    if os.path.exists(dest_file):
        os.remove(dest_file)


def execute_02_data():
    columns_ls = ['destin_name', 'sales_name', 'sales_addressphone', 'sales_bank', 'finance_travel_id']
    extra_columns_ls = ['bill_id']
    columns_ls.extend(extra_columns_ls)
    columns_str = ",".join(columns_ls)
    sql = """
    select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill where sales_name is not null or sales_addressphone is not null or sales_bank is not null 
    
    """.format(columns_str=columns_str).replace('\n', '').replace('\r', '').strip()

    log.info(sql)
    count_sql = 'select count(a.bill_id) from ({sql}) a'.format(sql=sql)
    log.info(count_sql)
    records = prod_execute_sql(conn_type='test', sqltype='select', sql=count_sql)
    count_records = records[0][0]

    max_size = 10 * 10000
    limit_size = 100000
    select_sql_ls = []

    log.info(f'* count_records ==> {count_records}')
    if count_records >= max_size:
        offset_size = 0
        while offset_size <= count_records:
            if offset_size + limit_size > count_records:
                limit_size = count_records - offset_size
                tmp_sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill where sales_name is not null or sales_addressphone is not null or sales_bank is not null order by finance_travel_id limit {limit_size} offset {offset_size}".format(
                    columns_str=columns_str, limit_size=limit_size, offset_size=offset_size)

                select_sql_ls.append(tmp_sql)
                break
            else:
                tmp_sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill where sales_name is not null or sales_addressphone is not null or sales_bank is not null order by finance_travel_id limit {limit_size} offset {offset_size}".format(
                    columns_str=columns_str, limit_size=limit_size, offset_size=offset_size)
                select_sql_ls.append(tmp_sql)

            offset_size = offset_size + limit_size
    else:
        tmp_sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill where sales_name is not null or sales_addressphone is not null or sales_bank is not null".format(
            columns_str=columns_str)
        select_sql_ls.append(tmp_sql)
        print('*** tmp_sql => ', tmp_sql)

    log.info('* 开始分页查询')

    obj_list = []
    threadPool = ThreadPoolExecutor(max_workers=20)
    start_time = time.perf_counter()

    init_file()

    for sel_sql in select_sql_ls:
        log.info(sel_sql)
        obj = threadPool.submit(exec_task, sel_sql)
        obj_list.append(obj)

    for future in as_completed(obj_list):
        data = future.result()

        if data and len(data) > 0:
            print(len(data))

            for record in data:
                sales_address = operate_reocrd(record)
                # print()

                destin_name = str(record[0])
                sales_name = str(record[1])
                sales_addressphone = str(record[2])
                sales_bank = str(record[3])
                finance_travel_id = str(record[4])

                record = f'{finance_travel_id},{sales_name},{sales_addressphone},{sales_bank},{sales_address}'
                print(record)

                with open(dest_file, "a", encoding='utf-8') as file:
                    file.write(record)
                    file.write("\n")

    threadPool.shutdown(wait=True)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* 查询耗时 {consumed_time} sec')


def operate_reocrd(record):
    destin_name = str(record[0])
    sales_name = str(record[1])
    sales_addressphone = str(record[2])
    sales_bank = str(record[3])

    # print('destin_name=',destin_name)
    # print('sales_name=', sales_name)
    # print('sales_addressphone=', sales_addressphone)
    # print('sales_bank=', sales_bank)

    if sales_name != 'None' and sales_addressphone != 'None' and sales_bank != 'None':

        # 只匹配市和县
        if sales_name != 'None':
            sales_name_city = match_address(place=sales_name, key='市') if match_address(place=sales_name,
                                                                                        key='市') else match_address(
                place=sales_name, key='县')

            if sales_name_city:
                # print('sales_address=', sales_name_city)
                return sales_name_city

        if sales_addressphone != 'None':
            sales_addressphone_city = match_address(place=sales_addressphone, key='市') if match_address(
                place=sales_addressphone, key='市') else match_address(place=sales_addressphone, key='县')
            if sales_addressphone_city:
                # print('sales_address=', sales_addressphone_city)
                return sales_addressphone_city

        if sales_bank != 'None':
            sales_bank_city = match_address(place=sales_bank, key='市') if match_address(place=sales_bank,
                                                                                        key='市') else match_address(
                place=sales_bank, key='县')
            if sales_bank_city:
                # print('sales_address=', sales_bank_city)
                return sales_bank_city


def exec_task(sql):
    records = prod_execute_sql(conn_type='test', sqltype='select', sql=sql)
    if records and len(records) > 0:
        return records
    else:
        return None


def main():
    execute_02_data()
    print('--- created txt file ---')

    test_hdfs = Test_HDFSTools(conn_type='test')
    test_hdfs.uploadFile2(hdfsDirPath=upload_hdfs_path, localPath=dest_file)

    os._exit(0)  # 无错误退出


if __name__ == "__main__":
    main()
