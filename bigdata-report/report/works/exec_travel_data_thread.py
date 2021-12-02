# -*- coding: utf-8 -*-
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
import os
import time
from report.commons.connect_kudu2 import prod_execute_sql

from report.commons.logging import get_logger
from report.commons.test_hdfs_tools import HDFSTools as Test_HDFSTools
from report.commons.tools import MatchArea, process_invalid_content
from report.services.common_services import ProvinceService, FinanceAdministrationService
import threading
from report.commons.settings import CONN_TYPE
import sys

"""

把上传的数据放到 02_logical_layer_007_h_lf_cw.finance_travel_linshi_analysis 表里

select * from  02_logical_layer_007_h_lf_cw.finance_travel_linshi_analysis

cd /you_filed_algos/app

PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_thread.py

PYTHONIOENCODING=utf-8 nohup /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_thread.py 2021 &
PYTHONIOENCODING=utf-8 nohup /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_thread.py 2020 &
PYTHONIOENCODING=utf-8 nohup /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_thread.py 2019 &
PYTHONIOENCODING=utf-8 nohup /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_thread.py 2018 &
PYTHONIOENCODING=utf-8 nohup /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_thread.py 2017 &
PYTHONIOENCODING=utf-8 nohup /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_thread.py 2016 &
PYTHONIOENCODING=utf-8 nohup /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_thread.py 2015 &
PYTHONIOENCODING=utf-8 nohup /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_thread.py 2014 &
PYTHONIOENCODING=utf-8 nohup /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_thread.py 2013 &

PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_thread.py 2021
PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_thread.py 2020
PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_thread.py 2019
PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_thread.py 2018
PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_thread.py 2017
PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_thread.py 2016
PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_thread.py 2015
PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_thread.py 2014
PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_thread.py 2013   没数据
PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_thread.py 2012   没数据
PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_thread.py 2011   没数据
PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_thread.py 2010   没数据


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
lock = threading.RLock()


def get_dest_file(year):
    dest_file = f"/you_filed_algos/prod_kudu_data/temp/travel_data_{year}.txt"
    return dest_file


def get_upload_hdfs_path(year):
    upload_hdfs_path = f'hdfs:///user/hive/warehouse/02_logical_layer_007_h_lf_cw.db/finance_travel_linshi_analysis/travel_data_{year}.txt'
    return upload_hdfs_path


def init_file(year):
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)

    dest_file = get_dest_file(year)
    if os.path.exists(dest_file):
        os.remove(dest_file)

    os.mknod(dest_file)


def execute_02_data(year):
    columns_ls = ['destin_name', 'sales_name', 'sales_addressphone', 'sales_bank', 'finance_travel_id', 'origin_name',
                  'invo_code', 'sales_taxno']

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
    limit_size = 5 * 1000
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

    if count_records >= 20000:
        max_workers = 10
    else:
        max_workers = 5

    log.info(f'*** 开始分页查询，一共 {len(select_sql_ls)} 页, max_workers={max_workers}')

    if count_records > 0:
        init_file(year)

        threadPool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="thr")
        start_time = time.perf_counter()

        all_task = [threadPool.submit(exec_task, sel_sql, year) for sel_sql in select_sql_ls]
        wait(all_task, return_when=ALL_COMPLETED)

        threadPool.shutdown(wait=True)
        consumed_time = round(time.perf_counter() - start_time)
        log.info(f'* 处理 {count_records} 条记录，共操作耗时 {consumed_time} sec, max_workers={max_workers}')

        # 上传文件到HDFS
        upload_hdfs_file(year)

    else:
        log.info(f'* 查询日期 => {year}， 没有查询到任何数据')


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
    if rst[1] is not None or rst[2] is not None:
        if rst[2] is not None:
            sales_address = rst[2]
            receipt_city = rst[1]
        elif rst[1] is not None:
            sales_address = rst[1]
            sales_address2 = match_area.query_sales_address(sales_name=sales_name,
                                                            sales_addressphone=sales_addressphone,
                                                            sales_bank=sales_bank)  # 发票开票地(最小行政)
            if sales_address2 is not None:
                sales_address = sales_address2

            if sales_address is None:
                sales_address = destin_name

            receipt_city = rst[1]

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

        # log.info(f'222 sales_address={sales_address},receipt_city={receipt_city}')

    return sales_address, receipt_city


def exec_task(sql, year_month):
    dest_file = get_dest_file(year_month)

    log.info(sql)
    start_time0 = time.perf_counter()
    records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sql)
    consumed_time0 = (time.perf_counter() - start_time0)
    log.info(f'* 取数耗时 => {consumed_time0} sec, records={len(records)}')

    # time.sleep(0.01)

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

            # start_time2 = time.perf_counter()
            sales_address, receipt_city = operate_every_record(record)
            # log.info(f" {threading.current_thread().name} is running ")
            # consumed_time2 = round(time.perf_counter() - start_time2)
            # log.info(
            #     f'* consumed_time2 => {consumed_time2} sec, sales_address={sales_address}, receipt_city={receipt_city}')
            #
            # start_time3 = time.perf_counter()

            # origin_province = match_area.query_belong_province(origin_name)  # 行程出发地(省)
            origin_province = province_service.query_belong_province(area_name=origin_name)  # 行程出发地(省)

            # consumed_time3 = round(time.perf_counter() - start_time3)
            # log.info(
            #     f'* consumed_time3 => {consumed_time3} sec, origin_name={origin_name}, origin_province={origin_province}')
            # start_time4 = time.perf_counter()

            destin_province = match_area.query_destin_province(invo_code=invo_code,
                                                               destin_name=destin_name)  # 行程目的地(省)

            # consumed_time4 = round(time.perf_counter() - start_time4)
            # log.info(
            #     f'* consumed_time4 => {consumed_time4} sec, destin_name={destin_name}, destin_province={destin_province}')

            origin_name = process_invalid_content(origin_name)  # 行程出发地(市)
            sales_name = process_invalid_content(sales_name)  # 开票公司
            sales_addressphone = process_invalid_content(sales_addressphone)  # 开票地址及电话
            sales_bank = process_invalid_content(sales_bank)  # 发票开户行
            invo_code = process_invalid_content(invo_code)  # 发票代码
            sales_address = match_area.filter_area(process_invalid_content(sales_address))  # 发票开票地(市)
            origin_province = process_invalid_content(origin_province)  # 行程出发地(省)
            destin_province = process_invalid_content(destin_province)  # 行程目的地(省)
            receipt_city = match_area.filter_area(process_invalid_content(receipt_city))  # 发票开票所在市
            destin_name = process_invalid_content(destin_name)
            sales_taxno = process_invalid_content(sales_taxno)
            account_period = year_month

            consumed_time1 = (time.perf_counter() - start_time1)
            log.info( f'* {threading.current_thread().name} 生成每行数据耗时 => {consumed_time1} sec, idx={idx}, year_month={year_month}')

            record_str = f'{finance_travel_id}\u0001{origin_name}\u0001{destin_name}\u0001{sales_name}\u0001{sales_addressphone}\u0001{sales_bank}\u0001{invo_code}\u0001{sales_taxno}\u0001{sales_address}\u0001{origin_province}\u0001{destin_province}\u0001{receipt_city}\u0001{account_period}'
            # print(record_str)
            # print('')

            result.append(record_str)

            if len(result) >= 100:
                lock.acquire()

                for item in result:
                    with open(dest_file, "a+", encoding='utf-8') as file:
                        file.write(item + "\n")
                result = []

                lock.release()

        if len(result) > 0:
            for item in result:
                with open(dest_file, "a+", encoding='utf-8') as file:
                    file.write(item + "\n")

        del result


def upload_hdfs_file(year):
    dest_file = get_dest_file(year)
    upload_hdfs_path = get_upload_hdfs_path(year)
    test_hdfs.uploadFile2(hdfsDirPath=upload_hdfs_path, localPath=dest_file)


def upload_hdfs_all_files():
    for year in ['2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020', '2021']:
        dest_file = get_dest_file(year)
        upload_hdfs_path = get_upload_hdfs_path(year)

        test_hdfs.uploadFile2(hdfsDirPath=upload_hdfs_path, localPath=dest_file)
        print(f'* upload the dest_file={dest_file}')


def main():
    """
    2021 年, 一共 3565021 条, 消耗时间      sec
    2020 年, 一共 4769258 条, 消耗时间      sec
    2019 年, 一共 4401235 条, 消耗时间      sec
    2018 年, 一共 3548757 条, 消耗时间      sec
    2017 年, 一共 2318286 条, 消耗时间      sec
    2016 年, 一共 1088516 条, 消耗时间   41055   sec
    """

    year = sys.argv[1]
    #year = '2015'
    execute_02_data(year)
    print('--- ok ---')


main()
