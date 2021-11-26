# -*- coding: utf-8 -*-
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
import os
import time
# from report.commons.connect_kudu import prod_execute_sql
from report.commons.connect_kudu2 import prod_execute_sql

from report.commons.logging import get_logger
from report.commons.test_hdfs_tools import HDFSTools as Test_HDFSTools
from report.commons.tools import MatchArea
from report.services.common_services import ProvinceService, FinanceAdministrationService
import threading
from report.commons.settings import CONN_TYPE
import sys


"""

把上传的数据放到 02_logical_layer_007_h_lf_cw.finance_travel_linshi_analysis 表里

cd /you_filed_algos/app

/root/anaconda3/bin/python -u /you_filed_algos/app/report/works/exec_travel_data.py

/root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data.py

PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_thread.py

PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_thread.py 2021
PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_thread.py 2020
PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_thread.py 2019
PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_thread.py 2018

PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_thread.py 2017
PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_thread.py 2016
PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_thread.py 2015
PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_thread.py 2014
PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_thread.py 2013
PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_thread.py 2012
PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_thread.py 2011
PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_thread.py 2010


select * from 02_logical_layer_007_h_lf_cw.finance_travel_linshi_analysis

"""

log = get_logger(__name__)

dest_dir = '/you_filed_algos/prod_kudu_data/temp'
dest_file = "/you_filed_algos/prod_kudu_data/temp/travel_data.txt"
upload_hdfs_path = 'hdfs:///user/hive/warehouse/02_logical_layer_007_h_lf_cw.db/finance_travel_linshi_analysis/travel_data.txt'

match_area = MatchArea()
province_service = ProvinceService()
finance_service = FinanceAdministrationService()
test_hdfs = Test_HDFSTools(conn_type=CONN_TYPE)

test_limit_cond = ' '  # 'LIMIT 10000'


def get_dest_file(year):
    dest_file = f"/you_filed_algos/prod_kudu_data/temp/travel_data_{year}.txt"
    return dest_file


def init_file(year):
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)

    dest_file = get_dest_file(year)
    if os.path.exists(dest_file):
        os.remove(dest_file)

    os.mknod(dest_file)


def execute_02_data(year):
    init_file(year)

    columns_ls = ['destin_name', 'sales_name', 'sales_addressphone', 'sales_bank', 'finance_travel_id', 'origin_name',
                  'invo_code', 'sales_taxno']

    # extra_columns_ls = ['bill_id']
    # columns_ls.extend(extra_columns_ls)

    columns_str = ",".join(columns_ls)
    sql = """
    select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill 
        where !(sales_name is  null and  sales_addressphone is null and sales_bank is null and origin_name is  null and destin_name is  null and sales_taxno is null ) and left(account_period,4) ='{year}' 
       {test_limit_cond}
    """.format(columns_str=columns_str, year=year, test_limit_cond=test_limit_cond).replace('\n', '').replace('\r',
                                                                                                              '').strip()

    log.info(sql)
    count_sql = 'select count(a.finance_travel_id) from ({sql}) a'.format(sql=sql)
    log.info(count_sql)
    records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=count_sql)
    count_records = records[0][0]

    max_size = 1 * 10000
    limit_size = 1 * 5000
    select_sql_ls = []

    log.info(f'* count_records ==> {count_records}')
    if count_records >= max_size:
        offset_size = 0
        while offset_size <= count_records:
            if offset_size + limit_size > count_records:
                limit_size = count_records - offset_size
                tmp_sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill where !(sales_name is  null and  sales_addressphone is null and sales_bank is null and origin_name is  null and  destin_name is  null and sales_taxno is null ) and left(account_period,4) ='{year}' order by jour_beg_date limit {limit_size} offset {offset_size}".format(
                    columns_str=columns_str, limit_size=limit_size, offset_size=offset_size, year=year)

                select_sql_ls.append(tmp_sql)
                break
            else:
                tmp_sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill where !(sales_name is  null and  sales_addressphone is null and sales_bank is null and origin_name is  null and  destin_name is null and sales_taxno is null ) and left(account_period,4) ='{year}'  order by jour_beg_date limit {limit_size} offset {offset_size}".format(
                    columns_str=columns_str, limit_size=limit_size, offset_size=offset_size, year=year)
                select_sql_ls.append(tmp_sql)

            offset_size = offset_size + limit_size
    else:
        tmp_sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill where !(sales_name is  null and  sales_addressphone is null and sales_bank is null and origin_name is  null and  destin_name is null and sales_taxno is null ) and left(account_period,4) ='{year}'  {test_limit_cond} ".format(
            columns_str=columns_str, test_limit_cond=test_limit_cond, year=year)
        select_sql_ls.append(tmp_sql)
        # print('*** tmp_sql => ', tmp_sql)

    log.info(f'*** 开始分页查询，一共 {len(select_sql_ls)} 页')

    # max_workers=40 , 每小时处理数据量
    # max_workers=60 , 每小时处理数据量
    threadPool = ThreadPoolExecutor(max_workers=40, thread_name_prefix="thr")
    start_time = time.perf_counter()

    # for sel_sql in select_sql_ls:
    #     log.info(sel_sql)
    # threadPool.submit(exec_task, sel_sql)

    all_task = [threadPool.submit(exec_task, sel_sql, year) for sel_sql in select_sql_ls]
    wait(all_task, return_when=ALL_COMPLETED)

    threadPool.shutdown(wait=True)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* 查询耗时 {consumed_time} sec')


def operate_every_record(record):
    destin_name = str(record[0]) if record[0] else None  # 行程目的地
    sales_name = str(record[1]) if record[1] else None  # 开票公司
    sales_addressphone = str(record[2]) if record[2] else None  # 开票地址及电话
    sales_bank = str(record[3]) if record[3] else None  # 发票开户行
    finance_travel_id = str(record[4]) if record[4] else None
    origin_name = str(record[5]) if record[5] else None  # 行程出发地
    invo_code = str(record[6]) if record[6] else None  # 发票代码
    sales_taxno = str(record[7]) if record[7] else None  # 纳税人识别号

    rst = finance_service.query_areas(sales_taxno=sales_taxno)
    # log.info(f'000 rst={rst}, rst[0]={rst[0]}, rst[1]={rst[1]}, rst[2]={rst[2]} ')
    # log.info(type(rst))

    sales_address, receipt_city = None, None
    if rst[0] is not None and rst[1] is not None:
        if rst[2] is not None:
            sales_address = rst[2]
            receipt_city = rst[1]
        elif rst[1] is not None:
            sales_address = rst[1]
            receipt_city = sales_address
        elif rst[0] is not None:
            sales_address = rst[0]

        # log.info(f'111 sales_address={sales_address},receipt_city={receipt_city}')

    else:
        sales_address = match_area.query_sales_address(sales_name=sales_name, sales_addressphone=sales_addressphone,
                                                       sales_bank=sales_bank)  # 发票开票地(最小行政)
        if sales_address is None:
            sales_address = destin_name

        receipt_city = match_area.query_receipt_city(sales_name=sales_name, sales_addressphone=sales_addressphone,
                                                     sales_bank=sales_bank)  # 发票开票所在市

        """
        1，优先从 开票公司，开票地址及电话和发票开户行 求得sales_address发票开票地(最小行政) 找到'开票地所在的市' 
        2，如果没有找到开票所在的市，就从'目的地'找到'开票所在的市' 

           如果没有找到从开票所在地最小的行政单位，找到开票所在地的市,会出现问题，比如多个市下可能会有相同的最小行政单位  
       """

        if receipt_city is None:
            receipt_city = match_area.query_receipt_city(sales_name=destin_name, sales_addressphone=None,
                                                         sales_bank=None)

        receipt_city = match_area.filter_area(receipt_city.replace(',', ' ')) if receipt_city else '无'
        # log.info(f'222 sales_address={sales_address},receipt_city={receipt_city}')

    return sales_address, receipt_city


def exec_task(sql, year):
    log.info(sql)
    #log.info(f'year={year}')

    start_time0 = time.perf_counter()

    records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sql)
    consumed_time0 = (time.perf_counter() - start_time0)
    log.info(f'* 取数耗时 => {consumed_time0} sec, records={len(records)}')

    time.sleep(0.01)

    dest_file = get_dest_file(year)

    if records and len(records) > 0:
        result = []

        for idx, record in enumerate(records):
            start_time1 = time.perf_counter()

            destin_name = str(record[0]) if record[0] else None  # 行程目的地
            sales_name = str(record[1]) if record[1] else None  # 开票公司
            sales_addressphone = str(record[2]) if record[2] else None  # 开票地址及电话
            sales_bank = str(record[3]) if record[3] else None  # 发票开户行
            finance_travel_id = str(record[4]) if record[4] else None
            origin_name = str(record[5]) if record[5] else None  # 行程出发地
            invo_code = str(record[6]) if record[6] else None  # 发票代码
            sales_taxno = str(record[7]) if record[7] else None  # 纳税人识别号

            # sales_address = match_area.query_sales_address(sales_name=sales_name, sales_addressphone=sales_addressphone,
            #                                                sales_bank=sales_bank)  # 发票开票地(最小行政)
            # if sales_address is None:
            #     sales_address = destin_name

            """
             1，优先从 开票公司，开票地址及电话和发票开户行 求得sales_address发票开票地(最小行政) 找到'开票地所在的市' 
             2，如果没有找到开票所在的市，就从'目的地'找到'开票所在的市' 
             
                如果没有找到从开票所在地最小的行政单位，找到开票所在地的市,会出现问题，比如多个市下可能会有相同的最小行政单位  
            """
            # receipt_city = match_area.query_receipt_city(sales_name=sales_name, sales_addressphone=sales_addressphone,
            #                                              sales_bank=sales_bank)  # 发票开票所在市
            # if receipt_city is None:
            #     receipt_city = match_area.query_receipt_city(sales_name=destin_name, sales_addressphone=None,
            #                                                  sales_bank=None)

            start_time2 = time.perf_counter()
            sales_address, receipt_city = operate_every_record(record)
            # log.info(f" {threading.current_thread().name} is running ")
            consumed_time2 = round(time.perf_counter() - start_time2)
            log.info(
                f'* consumed_time2 => {consumed_time2} sec, sales_address={sales_address}, receipt_city={receipt_city}')

            start_time3 = time.perf_counter()

            # origin_province = match_area.query_belong_province(origin_name)  # 行程出发地(省)
            origin_province = province_service.query_belong_province(area_name=origin_name)  # 行程出发地(省)

            consumed_time3 = round(time.perf_counter() - start_time3)
            log.info(
                f'* consumed_time3 => {consumed_time3} sec,origin_name={origin_name}，origin_province={origin_province}')

            start_time4 = time.perf_counter()

            destin_province = match_area.query_destin_province(invo_code=invo_code,
                                                               destin_name=destin_name)  # 行程目的地(省)

            consumed_time4 = round(time.perf_counter() - start_time4)
            log.info(f'* consumed_time4 => {consumed_time4} sec, destin_province={destin_province}')

            # print('222 destin_province => ', destin_province)
            # consumed_time2 = round(time.perf_counter() - start_time2)
            # log.info(f'* consumed_time2 => {consumed_time2} sec, idx={idx}, destin_province={destin_province}')

            origin_name = origin_name.replace(',', ' ') if origin_name else '无'  # 行程出发地(市)
            sales_name = sales_name.replace(',', ' ') if sales_name else '无'  # 开票公司
            sales_addressphone = sales_addressphone.replace(',', ' ') if sales_addressphone else '无'  # 开票地址及电话
            sales_bank = sales_bank.replace(',', ' ') if sales_bank else '无'  # 发票开户行
            invo_code = invo_code if invo_code else '无'  # 发票代码
            sales_address = sales_address if sales_address else '无'  # 发票开票地(市)
            origin_province = origin_province if origin_province else '无'  # 行程出发地(省)
            destin_province = destin_province if destin_province else '无'  # 行程目的地(省)
            # receipt_city = match_area.filter_area(receipt_city.replace(',', ' ')) if receipt_city else '无'
            receipt_city = receipt_city if receipt_city else '无'  # 发票开票所在市
            destin_name = destin_name.replace(',', ' ') if destin_name else '无'
            sales_taxno = sales_taxno.replace(',', ' ') if sales_taxno else '无'
            account_period = year

            consumed_time1 = (time.perf_counter() - start_time1)
            log.info(f'* {threading.current_thread().name} 生成每行数据耗时 => {consumed_time1} sec , idx={idx}')

            record_str = f'{finance_travel_id},{origin_name},{destin_name},{sales_name},{sales_addressphone},{sales_bank},{invo_code},{sales_taxno},{sales_address},{origin_province},{destin_province},{receipt_city}，{account_period}'
            # print(record_str)
            # print('')
            result.append(record_str)

            print()
            # time.sleep(0.01)

            # start_time2 = time.perf_counter()

            # with open(dest_file, "a+", encoding='utf-8') as file:
            #     file.write(record_str + "\n")

            if len(result) >= 100:

                for item in result:
                    with open(dest_file, "a+", encoding='utf-8') as file:
                        file.write(item + "\n")
                result = []

            # consumed_time2 = round(time.perf_counter() - start_time2)
            # log.info(f'* 每行数据存储耗时 => {consumed_time2} sec')

            # time.sleep(0.001)

        if len(result) > 0:
            for item in result:
                with open(dest_file, "a+", encoding='utf-8') as file:
                    file.write(item + "\n")
        del result

        test_hdfs.uploadFile2(hdfsDirPath=upload_hdfs_path, localPath=dest_file)


def main():

    """
    2021 年 , 一共    , 消耗时间     sec


    """
    year = sys.argv[1]
    execute_02_data(year)  # 一共 11926897  , 消耗时间     sec
    print(f'* created txt file dest_file={dest_file}')

    #test_hdfs = Test_HDFSTools(conn_type=CONN_TYPE)
    # test_hdfs.uploadFile2(hdfsDirPath=upload_hdfs_path, localPath=dest_file)





main()
