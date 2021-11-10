# -*- coding: utf-8 -*-
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
import os
import time
from report.commons.connect_kudu import prod_execute_sql
from report.commons.logging import get_logger
from report.commons.test_hdfs_tools import HDFSTools as Test_HDFSTools
from report.commons.tools import MatchArea
from report.services.common_services import ProvinceService
import threading

log = get_logger(__name__)

dest_dir = '/you_filed_algos/prod_kudu_data/temp'
dest_file = dest_dir + "/car_data.txt"
upload_hdfs_path = 'hdfs:///user/hive/warehouse/02_logical_layer_007_h_lf_cw.db/finance_car_linshi_analysis/car_data.txt'

match_area = MatchArea()
province_service = ProvinceService()

conn_type = 'prod'

def init_file():
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)

    if os.path.exists(dest_file):
        os.remove(dest_file)


def check_car_linshi_data():
    init_file()

    columns_ls = ['finance_car_id', 'sales_name', 'sales_addressphone', 'sales_bank']
    columns_str = ",".join(columns_ls)

    sql = """
        select {columns_str}
    from 01_datamart_layer_007_h_cw_df.finance_car_bill 
    where sales_name is not null or sales_addressphone is not null or sales_bank is not null  
        """.format(columns_str=columns_str)

    log.info(sql)
    count_sql = 'select count(a.finance_car_id) from ({sql}) a'.format(sql=sql)
    log.info(count_sql)
    records = prod_execute_sql(conn_type='test', sqltype='select', sql=count_sql)
    count_records = records[0][0]
    log.info(f'* count_records ==> {count_records}')

    max_size = 1 * 10000
    limit_size = 10000
    select_sql_ls = []

    if count_records >= max_size:
        offset_size = 0
        while offset_size <= count_records:
            if offset_size + limit_size > count_records:
                limit_size = count_records - offset_size

                tmp_sql = """
                    select {columns_str}
                from 01_datamart_layer_007_h_cw_df.finance_car_bill 
                where sales_name is not null or sales_addressphone is not null or sales_bank is not null 
                order by finance_car_id limit {limit_size} offset {offset_size}
                    """.format(columns_str=columns_str, limit_size=limit_size, offset_size=offset_size)

                select_sql_ls.append(tmp_sql)
                break
            else:
                tmp_sql = """
                    select {columns_str}
                from 01_datamart_layer_007_h_cw_df.finance_car_bill 
                where sales_name is not null or sales_addressphone is not null or sales_bank is not null 
                order by finance_car_id limit {limit_size} offset {offset_size}
                    """.format(columns_str=columns_str, limit_size=limit_size, offset_size=offset_size)

                select_sql_ls.append(tmp_sql)

            offset_size = offset_size + limit_size
    else:
        tmp_sql = """
            select {columns_str}
            from 01_datamart_layer_007_h_cw_df.finance_car_bill 
            where sales_name is not null or sales_addressphone is not null or sales_bank is not null 
            """.format(columns_str=columns_str)

        select_sql_ls.append(tmp_sql)
        print('*** tmp_sql => ', tmp_sql)

    log.info(f'*** 开始分页查询，一共 {len(select_sql_ls)} 页')

    threadPool = ThreadPoolExecutor(max_workers=10, thread_name_prefix="thr")
    start_time = time.perf_counter()

    all_task = [threadPool.submit(exec_task, (sel_sql)) for sel_sql in select_sql_ls]
    wait(all_task, return_when=ALL_COMPLETED)

    threadPool.shutdown(wait=True)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* 查询耗时 {consumed_time} sec')
    log.info('** 关闭线程池')


def exec_task(sql):
    records = prod_execute_sql(conn_type=conn_type, sqltype='select', sql=sql)
    if records and len(records) > 0:
        for idx, record in enumerate(records):
            finance_car_id = str(record[0])
            sales_name = str(record[1])  # 开票公司
            sales_addressphone = str(record[2])  # 开票地址及电话
            sales_bank = str(record[3])  # 发票开会行
            sales_address = operate_reocrd(record)  # 发票开票地(最小行政)
            receipt_city = province_service.query_receipt_city(sales_address)  # 发票开票所在市

            sales_name = sales_name.replace(',', ' ') if sales_name else '无'
            sales_addressphone = sales_addressphone.replace(',', ' ') if sales_addressphone else '无'
            sales_bank = sales_bank.replace(',', ' ') if sales_bank else '无'
            sales_address = sales_address.replace(',', ' ') if sales_address else '无'
            receipt_city = receipt_city.replace(',', ' ') if receipt_city else '无'

            log.info(f" {threading.current_thread().name} is doing ")
            record_str = f'{finance_car_id},{sales_name},{sales_addressphone},{sales_bank},{sales_address},{receipt_city}'
            print(record_str)
            print('')

            with open(dest_file, "a+", encoding='utf-8') as file:
                file.write(record_str + "\n")


def operate_reocrd(record):
    sales_name = str(record[1]) if record[1] else None  # 开票公司
    sales_addressphone = str(record[2]) if record[2] else None  # 开票地址及电话
    sales_bank = str(record[3]) if record[3] else None  # 发票开户行

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
    # show_str = f'{sales_name} , {sales_addressphone} , {sales_bank}, {result_area}'
    # print('### operate_reocrd show_str ==> ', show_str)

    return result_area


def main():
    check_car_linshi_data()  # 57350 50270

    test_hdfs = Test_HDFSTools(conn_type=conn_type)
    #test_hdfs.uploadFile2(hdfsDirPath=upload_hdfs_path, localPath=dest_file)

    os._exit(0)  # 无错误退出


if __name__ == "__main__":
    main()


