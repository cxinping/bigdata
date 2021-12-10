# -*- coding: utf-8 -*-

from gevent import monkey
monkey.patch_all()

import gevent
from gevent.pool import Pool

import sys
from report.commons.logging import get_logger
import time
import os
from report.commons.connect_kudu2 import prod_execute_sql
from report.commons.test_hdfs_tools import HDFSTools as Test_HDFSTools
from report.commons.tools import MatchArea, process_invalid_content
from report.services.common_services import ProvinceService, FinanceAdministrationService
import threading
from report.commons.settings import CONN_TYPE

"""

把上传的数据放到 02_logical_layer_007_h_lf_cw.finance_travel_linshi_analysis 表里

select * from  02_logical_layer_007_h_lf_cw.finance_travel_linshi_analysis

cd /you_filed_algos/app

PYTHONIOENCODING=utf-8 nohup /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_gevent.py 2021 &
PYTHONIOENCODING=utf-8 nohup /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_gevent.py 2020 &
PYTHONIOENCODING=utf-8 nohup /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_gevent.py 2019 &
PYTHONIOENCODING=utf-8 nohup /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_gevent.py 2018 &
PYTHONIOENCODING=utf-8 nohup /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_gevent.py 2017 &
PYTHONIOENCODING=utf-8 nohup /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_gevent.py 2016 &
PYTHONIOENCODING=utf-8 nohup /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_gevent.py 2015 &
PYTHONIOENCODING=utf-8 nohup /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_gevent.py 2014 &

PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_gevent.py 2021
PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_gevent.py 2020
PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_gevent.py 2019
PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_gevent.py 2018
PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_gevent.py 2017
PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_gevent.py 2016
PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_gevent.py 2015
PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_gevent.py 2014
PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_gevent.py 2013   没数据
PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_gevent.py 2012   没数据
PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_gevent.py 2011   没数据
PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/exec_travel_data_gevent.py 2010   没数据


"""

log = get_logger(__name__)

dest_dir = '/you_filed_algos/prod_kudu_data/temp'

match_area = MatchArea()
province_service = ProvinceService()
finance_service = FinanceAdministrationService()
test_hdfs = Test_HDFSTools(conn_type=CONN_TYPE)

test_limit_cond = ' '  # 'LIMIT 10000'


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

    max_size = 1 * 10001
    limit_size = 1 * 10000
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

    if count_records >= 20000:
        max_workers = 40
    else:
        max_workers = 5

    log.info(f'*** 开始分页查询，一共 {len(select_sql_ls)} 页, year={year}, max_workers={max_workers}')

    if count_records > 0:
        init_file(year)

        start_time = time.perf_counter()
        pool = Pool(30)

        results = []
        for sel_sql in select_sql_ls:
            rst = pool.spawn(exec_task, sel_sql, year)
            #rst = gevent.spawn(exec_task, sel_sql, year)
            results.append(rst)

        gevent.joinall(results)

        consumed_time = round(time.perf_counter() - start_time)
        log.info(f'* 处理 {count_records} 条记录，共操作耗时 {consumed_time} sec, year={year}')

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
    #log.info(f'000 rst={rst}, rst[0]={rst[0]}, rst[1]={rst[1]}, rst[2]={rst[2]} ')
    # log.info(type(rst))

    sales_address, receipt_city = None, None
    receipt_province = None  # receipt_province 发票开票所在省

    if rst[1] is not None or rst[2] is not None:
        receipt_province = rst[0]

        if rst[2] is not None:
            sales_address = rst[2]
            receipt_city = rst[1]
        elif rst[1] is not None:
            sales_address = rst[1]
            sales_address2 = match_area.query_sales_address_new(sales_name=sales_name,
                                                            sales_addressphone=sales_addressphone,
                                                            sales_bank=sales_bank)  # 发票开票地(最小行政)
            if sales_address2 is not None:
                sales_address = sales_address2

            if sales_address is None:
                sales_address = destin_name

            receipt_city = rst[1]

        #log.info(f'111 sales_address={sales_address},receipt_city={receipt_city}')

    else:
        sales_address = match_area.query_sales_address_new(sales_name=sales_name, sales_addressphone=sales_addressphone,
                                                       sales_bank=sales_bank)  # 发票开票地(最小行政)
        if sales_address is None:
            sales_address = destin_name

        if sales_address and '市' in sales_address:
            receipt_city = sales_address

            if receipt_province is None:
                receipt_province = province_service.query_belong_province(area_name=receipt_city)

            return sales_address, receipt_city, receipt_province

        receipt_city = match_area.query_receipt_city_new(sales_name=sales_name, sales_addressphone=sales_addressphone,
                                                     sales_bank=sales_bank)  # 发票开票所在市

        """
        1，优先从 开票公司，开票地址及电话和发票开户行 求得sales_address发票开票地(最小行政) 找到'开票地所在的市' 
        2，如果没有找到开票所在的市，就从'目的地'找到'开票所在的市' 
        
           如果没有找到从开票所在地最小的行政单位，找到开票所在地的市,会出现问题，比如多个市下可能会有相同的最小行政单位  
        """

        if receipt_city is None:
            receipt_city = match_area.query_receipt_city_new(sales_name=destin_name, sales_addressphone=None,
                                                         sales_bank=None)

        if receipt_province is None:
            receipt_province = province_service.query_belong_province(area_name=receipt_city)

        #log.info(f'222 sales_address={sales_address},receipt_city={receipt_city}')

    return sales_address, receipt_city, receipt_province


def exec_task(sql, year):
    dest_file = get_dest_file(year)

    log.info(sql)
    start_time0 = time.perf_counter()
    records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sql)
    consumed_time0 = (time.perf_counter() - start_time0)
    log.info(f'* 取数耗时 => {consumed_time0} sec, records={len(records)}')

    #gevent.sleep(1)

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

            sales_address, receipt_city, receipt_province = operate_every_record(record)

            origin_province = province_service.query_belong_province(area_name=origin_name)  # 行程出发地(省)

            # 优化方法
            destin_province = province_service.query_destin_province(invo_code=invo_code,
                                                               destin_name=destin_name)  # 行程目的地(省)

            consumed_time2 = round(time.perf_counter() - start_time1)
            if consumed_time2 >= 2:
                log.info(f'** 耗时 {consumed_time2} 秒')
                log.info(f'** sales_name={sales_name},sales_addressphone={sales_addressphone},sales_bank={sales_bank}')
                log.info(f'** sales_address={sales_address}, receipt_city={receipt_city}')
                #log.info(f'** origin_name={origin_name}, origin_province={origin_province}')
                #log.info(f'** invo_code={invo_code}, origin_province={destin_province}')
                gevent.sleep(0.5)

            # origin_province = None
            # destin_province = None

            origin_name = process_invalid_content(origin_name)  # 行程出发地(市)
            sales_name = process_invalid_content(sales_name)  # 开票公司
            sales_addressphone = process_invalid_content(sales_addressphone)  # 开票地址及电话
            sales_bank = process_invalid_content(sales_bank)  # 发票开户行
            invo_code = process_invalid_content(invo_code)  # 发票代码
            sales_address = match_area.filter_area(process_invalid_content(sales_address))  # 发票开票地(市)
            origin_province = process_invalid_content(origin_province)  # 行程出发地(省)
            destin_province = process_invalid_content(destin_province)  # 行程目的地(省)
            receipt_city = match_area.filter_area(process_invalid_content(receipt_city))  # 发票开票所在市
            receipt_province = match_area.filter_area(process_invalid_content(receipt_province))  # 发票开票所在省

            destin_name = process_invalid_content(destin_name)
            sales_taxno = process_invalid_content(sales_taxno)
            account_period = year

            consumed_time1 = (time.perf_counter() - start_time1)
            #log.info(f'* {threading.current_thread().name} 生成每行数据耗时 => {consumed_time1} sec, idx={idx}, year={year}')

            record_str = f'{finance_travel_id}\u0001{origin_name}\u0001{destin_name}\u0001{sales_name}\u0001{sales_addressphone}\u0001{sales_bank}\u0001{invo_code}\u0001{sales_taxno}\u0001{sales_address}\u0001{origin_province}\u0001{destin_province}\u0001{receipt_province}\u0001{receipt_city}\u0001{account_period}'
            # print(record_str)
            # print('')

            result.append(record_str)

            if len(result) >= 100:
                for item in result:
                    with open(dest_file, "a+", encoding='utf-8') as file:
                        file.write(item + "\n")
                result = []

        if len(result) > 0:
            for item in result:
                with open(dest_file, "a+", encoding='utf-8') as file:
                    file.write(item + "\n")

        del result


def upload_hdfs_file(year):
    dest_file = get_dest_file(year)
    upload_hdfs_path = get_upload_hdfs_path(year)
    test_hdfs.uploadFile2(hdfsDirPath=upload_hdfs_path, localPath=dest_file)


def main():
    """
    处理 50000 条记录，共操作耗时 314 sec, year=2021


    """

    year = sys.argv[1]
    #year = '2021'

    execute_02_data(year)
    print('--- ok ---')


main()