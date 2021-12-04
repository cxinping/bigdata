# -*- coding: utf-8 -*-
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
import os
import time
# from report.commons.connect_kudu import prod_execute_sql
from report.commons.connect_kudu2 import prod_execute_sql

from report.commons.logging import get_logger
from report.commons.test_hdfs_tools import HDFSTools as Test_HDFSTools
from report.commons.tools import MatchArea, process_invalid_content
from report.services.common_services import ProvinceService, FinanceAdministrationService
import threading
from report.commons.settings import CONN_TYPE

"""

把上传的数据放到 02_logical_layer_007_h_lf_cw.finance_meeting_linshi_analysis 表里

select * from  02_logical_layer_007_h_lf_cw.finance_meeting_linshi_analysis

"""

log = get_logger(__name__)

dest_dir = '/you_filed_algos/prod_kudu_data/temp'
dest_file = dest_dir + "/meeting_data.txt"
upload_hdfs_path = 'hdfs:///user/hive/warehouse/02_logical_layer_007_h_lf_cw.db/finance_meeting_linshi_analysis/meeting_data.txt'

match_area = MatchArea()
province_service = ProvinceService()
finance_service = FinanceAdministrationService()


def init_file():
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)

    if os.path.exists(dest_file):
        os.remove(dest_file)

    os.mknod(dest_file)


def check_meeting_data():
    init_file()

    columns_ls = ['finance_meeting_id', 'meet_addr', 'sales_name', 'sales_addressphone', 'sales_bank', 'sales_taxno']
    columns_str = ",".join(columns_ls)

    sql = """
        select {columns_str}
    from 01_datamart_layer_007_h_cw_df.finance_meeting_bill 
    where  !(sales_name is null and sales_addressphone is null and sales_bank is null and sales_taxno is null and meet_addr is null) 
        """.format(columns_str=columns_str)

    # log.info(sql)
    count_sql = 'select count(a.finance_meeting_id) from ({sql}) a'.format(sql=sql)
    log.info(count_sql)
    records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=count_sql)
    count_records = records[0][0]
    log.info(f'* count_records ==> {count_records}')

    max_size = 2 * 10000
    limit_size = 5000
    select_sql_ls = []

    if count_records >= max_size:
        offset_size = 0
        while offset_size <= count_records:
            if offset_size + limit_size > count_records:
                limit_size = count_records - offset_size

                tmp_sql = """
            select {columns_str}
            from 01_datamart_layer_007_h_cw_df.finance_meeting_bill 
            where !(sales_name is null and sales_addressphone is null and sales_bank is null and sales_taxno is null and meet_addr is null) 
                order by finance_meeting_id limit {limit_size} offset {offset_size}
                    """.format(columns_str=columns_str, limit_size=limit_size, offset_size=offset_size)

                select_sql_ls.append(tmp_sql)
                break
            else:
                tmp_sql = """
            select {columns_str}
            from 01_datamart_layer_007_h_cw_df.finance_meeting_bill 
            where !(sales_name is null and sales_addressphone is null and sales_bank is null and sales_taxno is null and meet_addr is null ) 
                order by finance_meeting_id limit {limit_size} offset {offset_size}
                    """.format(columns_str=columns_str, limit_size=limit_size, offset_size=offset_size)

                select_sql_ls.append(tmp_sql)

            offset_size = offset_size + limit_size
    else:
        tmp_sql = """
            select {columns_str}
            from 01_datamart_layer_007_h_cw_df.finance_meeting_bill 
            where !(sales_name is null and sales_addressphone is null and sales_bank is null and sales_taxno is null and meet_addr is null) 
            """.format(columns_str=columns_str)

        select_sql_ls.append(tmp_sql)
        # print('*** tmp_sql => ', tmp_sql)

    log.info(f'*** 开始分页查询，一共 {len(select_sql_ls)} 页')

    threadPool = ThreadPoolExecutor(max_workers=10, thread_name_prefix="thr")
    start_time = time.perf_counter()

    all_task = [threadPool.submit(exec_task, (sel_sql)) for sel_sql in select_sql_ls]
    wait(all_task, return_when=ALL_COMPLETED)

    threadPool.shutdown(wait=True)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* 操作耗时 {consumed_time} sec')


def operate_every_record(record):
    # if 1 == 1:
    #     return None, None

    # print()

    finance_meeting_id = str(record[0])
    meet_addr = str(record[1])           # 会议地址
    sales_name = str(record[2])          # 开票公司
    sales_addressphone = str(record[3])  # 开票地址及电话
    sales_bank = str(record[4])          # 发票开会行
    sales_taxno = str(record[5])         # 纳税人识别号

    # log.info(f'000 sales_taxno={sales_taxno}')
    rst = finance_service.query_areas(sales_taxno=sales_taxno)
    #log.info(f'000 rst={rst}, rst[0]={rst[0]}, rst[1]={rst[1]}, rst[2]={rst[2]} ')
    # log.info(type(rst))

    sales_address, receipt_city = None, None
    if rst[1] is not None or rst[2] is not None:
        if rst[2] is not None:
            sales_address = rst[2]
            receipt_city = rst[1]
        elif rst[1] is not None:
            sales_address = rst[1]
            sales_address2 = match_area.query_sales_address_new(sales_name=sales_name, sales_addressphone=sales_addressphone,
                                                           sales_bank=sales_bank)  # 发票开票地(最小行政)
            if sales_address2 is not None:
                sales_address = sales_address2

            receipt_city = rst[1]
            #receipt_city = sales_address

        log.info(f'111 sales_address={sales_address},receipt_city={receipt_city}')
    else:
        sales_address = match_area.query_sales_address_new(sales_name=sales_name, sales_addressphone=sales_addressphone,
                                                       sales_bank=sales_bank)  # 发票开票地(最小行政)

        if sales_address and '市' in sales_address:
            receipt_city = sales_address
            return sales_address, receipt_city

        receipt_city = match_area.query_receipt_city_new(sales_name=sales_name, sales_addressphone=sales_addressphone,
                                                     sales_bank=sales_bank)  # 发票开票所在市

        #log.info(f'222 sales_address={sales_address},receipt_city={receipt_city}')

        if sales_address is None and receipt_city is None:
            sales_address = match_area.query_sales_address_new(sales_name=meet_addr, sales_addressphone=None,
                                                           sales_bank=None)  # 发票开票地(最小行政)
            receipt_city = match_area.query_receipt_city_new(sales_name=meet_addr, sales_addressphone=None,
                                                         sales_bank=None)  # 发票开票所在市


    return sales_address, receipt_city


def exec_task(sql):
    records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sql)

    # log.info(sql)

    if records and len(records) > 0:
        result = []

        for idx, record in enumerate(records):
            finance_meeting_id = str(record[0])
            meet_addr = str(record[1])  # 会议地址
            sales_name = str(record[2])  # 开票公司
            sales_addressphone = str(record[3])  # 开票地址及电话
            sales_bank = str(record[4])  # 发票开会行
            sales_taxno = str(record[5])  # 纳税人识别号

            # sales_address = match_area.query_sales_address(sales_name=sales_name, sales_addressphone=sales_addressphone,
            #                                                sales_bank=sales_bank)  # 发票开票地(最小行政)
            #
            # receipt_city = match_area.query_receipt_city(sales_name=sales_name, sales_addressphone=sales_addressphone,
            #                                              sales_bank=sales_bank)  # 发票开票所在市

            sales_address, receipt_city = operate_every_record(record)

            # sales_taxno = sales_taxno.replace(',', ' ') if sales_taxno else '无'
            # meet_addr = meet_addr.replace(',', ' ') if meet_addr else '无'
            # sales_name = sales_name.replace(',', ' ') if sales_name else '无'
            # sales_addressphone = sales_addressphone.replace(',', ' ') if sales_addressphone else '无'
            # sales_bank = sales_bank.replace(',', ' ') if sales_bank else '无'
            # sales_address = sales_address.replace(',', ' ') if sales_address else '无'
            # receipt_city = match_area.filter_area(receipt_city.replace(',', ' ')) if receipt_city else '无'


            sales_taxno = process_invalid_content(sales_taxno)
            meet_addr = process_invalid_content(meet_addr)
            sales_name = process_invalid_content(sales_name)
            sales_addressphone = process_invalid_content(sales_addressphone)
            sales_bank = process_invalid_content(sales_bank)
            sales_address = match_area.filter_area(process_invalid_content(sales_address))
            receipt_city = match_area.filter_area(process_invalid_content(receipt_city))
            account_period = '无'

            log.info(f" {threading.current_thread().name} is running ")
            record_str = f'{finance_meeting_id}\u0001{sales_taxno}\u0001{meet_addr}\u0001{sales_name}\u0001{sales_addressphone}\u0001{sales_bank}\u0001{sales_address}\u0001{receipt_city}\u0001{account_period}'
            result.append(record_str)

            # print(record_str)
            # print('')

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
        # log.info(f"*** {threading.current_thread().name} has completed ")
        # print()


def main():
    # 一共 66880 , 消耗时间  1443   sec
    check_meeting_data()

    test_hdfs = Test_HDFSTools(conn_type=CONN_TYPE)
    test_hdfs.uploadFile2(hdfsDirPath=upload_hdfs_path, localPath=dest_file)

    print('--- ok ---')


if __name__ == "__main__":
    main()
