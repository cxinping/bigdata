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

"""

把上传的数据放到 02_logical_layer_007_h_lf_cw.finance_travel_linshi_analysis 表里
"""

log = get_logger(__name__)

dest_dir = '/you_filed_algos/prod_kudu_data/checkpoint25'
dest_file = dest_dir + "/check_25_meeting_data.txt"
upload_hdfs_path = 'hdfs:///user/hive/warehouse/02_logical_layer_007_h_lf_cw.db/finance_meeting_linshi_analysis/check_25_meeting_data.txt'

match_area = MatchArea()
province_service = ProvinceService()


def init_file():
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)

    if os.path.exists(dest_file):
        os.remove(dest_file)


def check_25_data():
    init_file()

    columns_ls = ['finance_meeting_id', 'meet_addr', 'sales_name', 'sales_addressphone', 'sales_bank']
    columns_str = ",".join(columns_ls)

    sql = """
        select {columns_str}
    from 01_datamart_layer_007_h_cw_df.finance_meeting_bill 
    where meet_addr is not null and (sales_name is not null or sales_addressphone is not null or sales_bank is not null )
        """.format(
        columns_str=columns_str)

    records = prod_execute_sql(conn_type='test', sqltype='select', sql=sql)
    if records and len(records) > 0:
        for idx, record in enumerate(records):
            finance_meeting_id = str(record[0])
            meet_addr = str(record[1])  # 会议地址
            sales_name = str(record[2])  # 开票公司
            sales_addressphone = str(record[3])  # 开票地址及电话
            sales_bank = str(record[4])  # 发票开会行
            sales_address = operate_reocrd(record)  # 发票开票地(市)

            meet_addr = meet_addr.replace(',', ' ') if meet_addr else 'null'
            sales_name = sales_name.replace(',', ' ') if sales_name else 'null'
            sales_addressphone = sales_addressphone.replace(',', ' ') if sales_addressphone else 'null'
            sales_bank = sales_bank.replace(',', ' ') if sales_bank else 'null'
            sales_address = sales_address.replace(',', ' ') if sales_address else 'null'

            record_str = f'{finance_meeting_id},{meet_addr},{sales_name},{sales_addressphone},{sales_bank},{sales_address}'
            print(record_str)
            print('')

            with open(dest_file, "a+", encoding='utf-8') as file:
                file.write(record_str + "\n")


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

    # show_str = f'{sales_name} , {sales_addressphone} , {sales_bank}, {result_area}'
    # print('### operate_reocrd show_str ==> ', show_str)

    return result_area


def main():
    check_25_data()

    # test_hdfs = Test_HDFSTools(conn_type='test')
    # test_hdfs.uploadFile2(hdfsDirPath=upload_hdfs_path, localPath=dest_file)

    # os._exit(0)  # 无错误退出


main()
