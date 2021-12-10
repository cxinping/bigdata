# -*- coding: utf-8 -*-
from gevent import monkey
import gevent
import threading

import random
import time
import os
import time
#from report.commons.connect_kudu import prod_execute_sql
from report.commons.connect_kudu2 import prod_execute_sql

from report.commons.logging import get_logger
from report.commons.test_hdfs_tools import HDFSTools as Test_HDFSTools
from report.commons.tools import MatchArea
from report.services.common_services import ProvinceService

log = get_logger(__name__)

dest_dir = '/you_filed_algos/prod_kudu_data/temp/'
dest_file = "/you_filed_algos/prod_kudu_data/temp/travel_data2.txt"
upload_hdfs_path = 'hdfs:///user/hive/warehouse/02_logical_layer_007_h_lf_cw.db/finance_travel_linshi_analysis/travel_data.txt'
error_file = "/you_filed_algos/prod_kudu_data/checkpoint2/error_data.txt"

match_area = MatchArea()
province_service = ProvinceService()


def init_file():
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)

    if os.path.exists(dest_file):
        os.remove(dest_file)


def execute_02_data():
    init_file()

    columns_ls = ['destin_name', 'sales_name', 'sales_addressphone', 'sales_bank', 'finance_travel_id', 'origin_name',
                  'invo_code']
    # extra_columns_ls = ['bill_id']
    # columns_ls.extend(extra_columns_ls)

    columns_str = ",".join(columns_ls)
    sql = """
       select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill where sales_name is not null or sales_addressphone is not null or sales_bank is not null 
       """.format(columns_str=columns_str).replace('\n', '').replace('\r', '').strip()

    log.info(sql)
    count_sql = 'select count(a.finance_travel_id) from ({sql}) a'.format(sql=sql)
    log.info(count_sql)
    records = prod_execute_sql(conn_type='test', sqltype='select', sql=count_sql)
    count_records = records[0][0]
    log.info(f'* count_records ==> {count_records}')

    max_size = 10 * 10000
    limit_size = 1 * 1000
    select_sql_ls = []

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
        tmp_sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill where sales_name is not null or sales_addressphone is not null or sales_bank is not null ".format(
            columns_str=columns_str)
        select_sql_ls.append(tmp_sql)
        print('*** tmp_sql => ', tmp_sql)

    log.info(f'*** 开始分页查询，一共 {len(select_sql_ls)} 页')
    async_operate(select_sql_ls)


def async_operate(select_sql_ls):
    log.info('****** begin async_operate  ***')

    gevent_ls = []
    for sel_sql in select_sql_ls:
        g_obj = gevent.spawn(exec_task, sel_sql)
        gevent_ls.append(g_obj)

    for g_obj in gevent_ls:
        g_obj.join()

    log.info('****** end async_operate  ***')

    #tasks = [gevent.spawn(exec_task, sel_sql) for sel_sql in select_sql_ls]

    #gevent.joinall(tasks)


def exec_task(sql):
    #log.info(f" {gevent.getcurrent()} is doing , {sql}")
    #gevent.sleep(1)

    records = prod_execute_sql(conn_type='test', sqltype='select', sql=sql)

    if records and len(records) > 0:
        for idx, record in enumerate(records):
            log.info(f" {gevent.getcurrent()} is running ")
            #gevent.sleep(0.01)

            start_time0 = time.perf_counter()
            sales_address = operate_reocrd(record)  # 发票开票地(最小行政)
            consumed_time0 = round(time.perf_counter() - start_time0)
            log.info(f'* consumed_time0 => {consumed_time0} sec, sales_address={sales_address}')

            destin_name = str(record[0]) if record[0] else None  # 行程目的地
            sales_name = str(record[1]) if record[1] else None  # 开票公司
            sales_addressphone = str(record[2]) if record[2] else None  # 开票地址及电话
            sales_bank = str(record[3]) if record[3] else None  # 发票开户行
            finance_travel_id = str(record[4]) if record[4] else None
            origin_name = str(record[5]) if record[5] else None  # 行程出发地(市)
            invo_code = str(record[6]) if record[6] else None  # 发票代码

            receipt_city = province_service.query_receipt_city(sales_address)  # 发票开票所在市

            start_time1 = time.perf_counter()
            # origin_province = match_area.query_belong_province(origin_name)  # 行程出发地(省)
            origin_province = province_service.query_belong_province(area_name=origin_name)  # 行程出发地(省)

            consumed_time1 = round(time.perf_counter() - start_time1)
            log.info(
                f'* consumed_time1 => {consumed_time1} sec, idx={idx}, origin_name={origin_name}, origin_province={origin_province}')

            start_time2 = time.perf_counter()
            destin_province = match_area.query_destin_province(invo_code=invo_code,
                                                               destin_name=destin_name)  # 行程目的地(省)
            # print('222 destin_province => ', destin_province)
            consumed_time2 = round(time.perf_counter() - start_time2)
            log.info(f'* consumed_time2 => {consumed_time2} sec, idx={idx}, destin_province={destin_province}')

            origin_name = origin_name.replace(',', ' ') if origin_name else '无'    # 行程出发地(市)
            sales_name = sales_name.replace(',', ' ') if sales_name else '无'       # 开票公司
            sales_addressphone = sales_addressphone.replace(',', ' ') if sales_addressphone else '无'  # 开票地址及电话
            sales_bank = sales_bank.replace(',', ' ') if sales_bank else '无'       # 发票开户行
            invo_code = invo_code if invo_code else '无'                            # 发票代码
            sales_address = sales_address if sales_address else '无'                # 发票开票地(市)
            origin_province = origin_province if origin_province else '无'          # 行程出发地(省)
            destin_province = destin_province if destin_province else '无'          # 行程目的地(省)
            receipt_city = receipt_city.replace(',', ' ') if receipt_city else '无' # 发票开票所在市

            record_str = f'{finance_travel_id},{origin_name},{sales_name},{sales_addressphone},{sales_bank},{invo_code},{sales_address},{origin_province},{destin_province},{receipt_city}'
            print(record_str)
            print('')

            with open(dest_file, "a+", encoding='utf-8') as file:
                file.write(record_str + "\n")

            #gevent.sleep(0.01)


def operate_reocrd(record):
    # destin_name = str(record[0]) if record[0] else None
    sales_name = str(record[1]) if record[1] else None  # 开票公司
    sales_addressphone = str(record[2]) if record[2] else None  # 开票地址及电话
    sales_bank = str(record[3]) if record[3] else None  # 发票开户行

    # print('destin_name=',destin_name)
    # print('sales_name=', sales_name)
    # print('sales_addressphone=', sales_addressphone)
    # print('sales_bank=', sales_bank)

    area_name1, area_name2, area_name3 = None, None, None
    if sales_name != 'None' or sales_name is not None:
        area_name1 = match_area.fit_area(area=sales_name)

    if sales_addressphone != 'None' or sales_addressphone is not None:
        area_name2 = match_area.fit_area(area=sales_addressphone)

    if sales_bank != 'None' or sales_bank is not None:
        area_name3 = match_area.fit_area(area=sales_bank)

    area_names = []
    if area_name1[0]:
        area_names.append(area_name1)

    if area_name2[0]:
        area_names.append(area_name2)

    if area_name3[0]:
        area_names.append(area_name3)

    result_area = match_area.opera_areas(area_names)

    show_str = f'### sales_name={sales_name}, sales_addressphone={sales_addressphone}, sales_bank={sales_bank}, sales_address={result_area}'
    print(show_str)

    return result_area


if __name__ == "__main__":
    execute_02_data()  # 1013121

    print("the main thread!")