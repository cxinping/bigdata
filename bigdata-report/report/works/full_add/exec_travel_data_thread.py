# -*- coding: utf-8 -*-

from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED

import sys
from report.commons.logging import get_logger
import time
import os
from report.commons.connect_kudu2 import prod_execute_sql
from report.commons.test_hdfs_tools import HDFSTools as Test_HDFSTools
from report.commons.tools import MatchArea, process_invalid_content, is_chinese
from report.services.common_services import ProvinceService, FinanceAdministrationService
import threading
from report.commons.settings import CONN_TYPE

"""

把上传的数据放到 02_logical_layer_007_h_lf_cw.finance_travel_linshi_analysis 表里

select * from  02_logical_layer_007_h_lf_cw.finance_travel_linshi_analysis

cd /you_filed_algos/app

PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/full_add/exec_travel_data_thread.py 2021013
PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/full_add/exec_travel_data_thread.py 2021012
PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/full_add/exec_travel_data_thread.py 2021011
PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/full_add/exec_travel_data_thread.py 2021010
PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/full_add/exec_travel_data_thread.py 2021009
PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/full_add/exec_travel_data_thread.py 2021008
PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/full_add/exec_travel_data_thread.py 2021007
PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/full_add/exec_travel_data_thread.py 2021006
PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/full_add/exec_travel_data_thread.py 2021005
PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/full_add/exec_travel_data_thread.py 2021004
PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/full_add/exec_travel_data_thread.py 2021003
PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/full_add/exec_travel_data_thread.py 2021002
PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/works/full_add/exec_travel_data_thread.py 2021001


"""

log = get_logger(__name__)

dest_dir = '/you_filed_algos/prod_kudu_data/temp'

match_area = MatchArea()
province_service = ProvinceService()
finance_service = FinanceAdministrationService()

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


def execute_02_data(year_month):
    columns_ls = ['destin_name', 'sales_name', 'sales_addressphone', 'sales_bank', 'finance_travel_id', 'origin_name',
                  'invo_code', 'sales_taxno']

    columns_str = ",".join(columns_ls)
    sql = """
    select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill 
        where !(sales_name is  null and  sales_addressphone is null and sales_bank is null and origin_name is  null and destin_name is  null and sales_taxno is null ) and pstng_date ='{year_month}' 
       {test_limit_cond}
    """.format(columns_str=columns_str, year_month=year_month, test_limit_cond=test_limit_cond).replace('\n',
                                                                                                        '').replace(
        '\r', '').strip()

    log.info(sql)
    count_sql = 'select count(a.finance_travel_id) from ({sql}) a'.format(sql=sql)
    log.info(count_sql)
    records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=count_sql)
    count_records = records[0][0]

    max_size = 1 * 10001
    limit_size = 1 * 20000
    select_sql_ls = []

    log.info(f'* count_records ==> {count_records}')
    if count_records >= max_size:
        offset_size = 0
        while offset_size <= count_records:
            if offset_size + limit_size > count_records:
                limit_size = count_records - offset_size
                tmp_sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill where !(sales_name is  null and  sales_addressphone is null and sales_bank is null and origin_name is  null and  destin_name is  null and sales_taxno is null ) and pstng_date ='{year_month}' order by jour_beg_date limit {limit_size} offset {offset_size}".format(
                    columns_str=columns_str, limit_size=limit_size, offset_size=offset_size, year_month=year_month)

                select_sql_ls.append(tmp_sql)
                break
            else:
                tmp_sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill where !(sales_name is  null and  sales_addressphone is null and sales_bank is null and origin_name is  null and  destin_name is null and sales_taxno is null ) and pstng_date ='{year_month}'  order by jour_beg_date limit {limit_size} offset {offset_size}".format(
                    columns_str=columns_str, limit_size=limit_size, offset_size=offset_size, year_month=year_month)
                select_sql_ls.append(tmp_sql)

            offset_size = offset_size + limit_size
    else:
        tmp_sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill where !(sales_name is  null and  sales_addressphone is null and sales_bank is null and origin_name is  null and  destin_name is null and sales_taxno is null ) and pstng_date ='{year_month}'  {test_limit_cond} ".format(
            columns_str=columns_str, test_limit_cond=test_limit_cond, year_month=year_month)
        select_sql_ls.append(tmp_sql)

    if count_records >= 20000:
        max_workers = 10
    else:
        max_workers = 1

    log.info(f'*** 开始分页查询，一共 {len(select_sql_ls)} 页, year_month={year_month}, max_workers={max_workers}')

    if count_records > 0:
        init_file(year_month)

        start_time = time.perf_counter()

        threadPool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="thr-")
        all_task = [threadPool.submit(exec_task, sel_sql, year_month) for sel_sql in select_sql_ls]
        wait(all_task, return_when=ALL_COMPLETED)
        threadPool.shutdown(wait=True)

        consumed_time = round(time.perf_counter() - start_time)
        log.info(f'* 处理 {count_records} 条记录，共操作耗时 {consumed_time} sec, year_month={year_month}')

        # 上传文件到HDFS
        upload_hdfs_file(year_month)

        # 刷新临时表
        refresh_linshi_table()

        #init_file(year_month)
    else:
        log.info(f'* 查询日期 => {year_month}， 没有查询到任何数据')


def operate_every_record(record):

    destin_name = str(record[0]) if record[0] else None  # 行程目的地
    sales_name = str(record[1]) if record[1] else None  # 开票公司
    sales_addressphone = str(record[2]) if record[2] else None  # 开票地址及电话
    sales_bank = str(record[3]) if record[3] else None  # 发票开户行
    finance_travel_id = str(record[4]) if record[4] else None
    origin_name = str(record[5]) if record[5] else None  # 行程出发地
    invo_code = str(record[6]) if record[6] else None  # 发票代码
    sales_taxno = str(record[7]) if record[7] else None  # 纳税人识别号

    sales_address, receipt_city = None, None  # 发票所在地的最小行政单位，发票所在地所在的市
    receipt_province = None  # receipt_province 发票开票所在省

    if (sales_taxno is None or len(sales_taxno) == 0) and (invo_code is None or len(invo_code) == 0):
        sales_address = destin_name
        receipt_city = match_area.query_receipt_city_new(sales_name=destin_name, sales_addressphone=None,
                                                         sales_bank=None)
        receipt_province = province_service.query_belong_province(area_name=receipt_city)
        return sales_address, receipt_city, receipt_province
    elif sales_taxno and len(sales_taxno) in [15, 20, 18]:
        rst = finance_service.query_areas(sales_taxno=sales_taxno)
        # log.info(f'000 rst={rst}, rst[0]={rst[0]}, rst[1]={rst[1]}, rst[2]={rst[2]} ')
        # log.info(type(rst))

        # 情况1，没有从纳税人识别号获得，开票所在的 省，市，区/县
        if rst[0] is None and rst[1] is None and rst[1] is None:
            sales_address = match_area.query_sales_address_new(sales_name=sales_name,
                                                               sales_addressphone=sales_addressphone,
                                                               sales_bank=sales_bank)  # 发票开票地(最小行政)
            # if sales_address is None:
            #     sales_address = destin_name

            receipt_city = match_area.query_receipt_city_new(sales_name=sales_name,
                                                             sales_addressphone=sales_addressphone,
                                                             sales_bank=sales_bank)
            if sales_address is not None:
                receipt_province = province_service.query_belong_province(area_name=sales_address)
                if receipt_province is None:
                    receipt_province = province_service.query_belong_province(area_name=receipt_city)
            else:
                receipt_province = province_service.query_belong_province(area_name=receipt_city)

        # 情况2 开票所在省不为空
        if rst[0] is not None:
            receipt_province = rst[0]
            sales_address = match_area.query_sales_address_new(sales_name=sales_name,
                                                               sales_addressphone=sales_addressphone,
                                                               sales_bank=sales_bank)  # 发票开票地(最小行政)
            # if sales_address is None:
            #     sales_address = destin_name

            receipt_city = match_area.query_receipt_city_new(sales_name=sales_name,
                                                             sales_addressphone=sales_addressphone,
                                                             sales_bank=sales_bank)
            # if receipt_city is None:
            #     receipt_city = sales_address

        # 情况3 开票所在的 市 或 区/县，有一个不为空
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

                # if sales_address is None:
                #     sales_address = destin_name

                receipt_city = rst[1]

            if receipt_province is None:
                if sales_address is not None:
                    receipt_province = province_service.query_belong_province(area_name=sales_address)
                    if receipt_province is None:
                        receipt_province = province_service.query_belong_province(area_name=receipt_city)
                else:
                    receipt_province = province_service.query_belong_province(area_name=receipt_city)

            # log.info(f'111 sales_address={sales_address},receipt_city={receipt_city}')
        return sales_address, receipt_city, receipt_province
    return None, None, None


def exec_task(sql, year_month):
    dest_file = get_dest_file(year_month)

    #log.info(sql)
    start_time0 = time.perf_counter()
    records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sql)
    consumed_time0 = (time.perf_counter() - start_time0)
    log.info(f'* 取数耗时 => {consumed_time0} sec, records={len(records)}')

    log.info('当前正在执行 线程=> ' + threading.current_thread().name + f' 的任务,一批任务数量是 {len(records)} ')
    log.info(f'year_month={year_month}, sql={sql}')

    if records and len(records) > 0:
        time.sleep(0.1)

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

            # 根据行程目的地和发票代码找到行程所在的省
            # destin_province = province_service.query_destin_province(invo_code=invo_code,
            #                                                          destin_name=destin_name)  # 行程目的地(省)
            destin_province = province_service.query_belong_province(area_name=destin_name)  # 行程目的地(省)

            consumed_time2 = round(time.perf_counter() - start_time1)
            if consumed_time2 >= 2:
                log.info(f'** 耗时 {consumed_time2} 秒')
                log.info(f'** sales_name={sales_name},sales_addressphone={sales_addressphone},sales_bank={sales_bank}')
                log.info(f'** sales_address={sales_address}, receipt_city={receipt_city}')
                # log.info(f'** origin_name={origin_name}, origin_province={origin_province}')
                # log.info(f'** invo_code={invo_code}, origin_province={destin_province}')

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
            pstng_date = year_month

            #consumed_time1 = (time.perf_counter() - start_time1)
            # log.info(f'* {threading.current_thread().name} 生成每行数据耗时 => {consumed_time1} sec, idx={idx}, year={year}')

            record_str = f'{finance_travel_id}\u0001{origin_name}\u0001{destin_name}\u0001{sales_name}\u0001{sales_addressphone}\u0001{sales_bank}\u0001{invo_code}\u0001{sales_taxno}\u0001{sales_address}\u0001{origin_province}\u0001{destin_province}\u0001{receipt_province}\u0001{receipt_city}\u0001{pstng_date}'
            # print(record_str)
            # print('')

            result.append(record_str)

            if len(result) >= 200:
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
    test_hdfs = Test_HDFSTools(conn_type=CONN_TYPE)
    test_hdfs.uploadFile2(hdfsDirPath=upload_hdfs_path, localPath=dest_file)


def refresh_linshi_table():
    sql = 'REFRESH 02_logical_layer_007_h_lf_cw.finance_travel_linshi_analysis'
    prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)


def main():
    """


    """

    year_month = sys.argv[1]
    # year = '2021012'
    execute_02_data(year_month)

    print('--- ok ---')


if __name__ == "__main__":
    main()