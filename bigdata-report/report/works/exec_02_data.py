# -*- coding: utf-8 -*-
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import time
from report.commons.connect_kudu import prod_execute_sql
from report.commons.logging import get_logger
from report.commons.test_hdfs_tools import HDFSTools as Test_HDFSTools
from report.commons.tools import MatchArea

"""

把上传的数据放到 02_logical_layer_007_h_lf_cw.finance_travel_linshi_analysis 表里
"""

log = get_logger(__name__)

dest_file = "/you_filed_algos/prod_kudu_data/check_02_trip_data.txt"
upload_hdfs_path = '/user/hive/warehouse/02_logical_layer_007_h_lf_cw.db/finance_travel_linshi_analysis/'

match_area = MatchArea()

test_limit_cond = ' LIMIT 10000 '  # 'LIMIT 10000'


def init_file():
    if os.path.exists(dest_file):
        os.remove(dest_file)


def execute_02_data():
    columns_ls = ['destin_name', 'sales_name', 'sales_addressphone', 'sales_bank', 'finance_travel_id', 'origin_name',
                  'invo_code']
    extra_columns_ls = ['bill_id']
    columns_ls.extend(extra_columns_ls)
    columns_str = ",".join(columns_ls)
    sql = """
    select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill where sales_name is not null or sales_addressphone is not null or sales_bank is not null 
    {test_limit_cond}}
    """.format(columns_str=columns_str, test_limit_cond=test_limit_cond).replace('\n', '').replace('\r', '').strip()

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
        tmp_sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill where sales_name is not null or sales_addressphone is not null or sales_bank is not null {test_limit_cond} ".format(
            columns_str=columns_str)
        select_sql_ls.append(tmp_sql)
        print('*** tmp_sql => ', tmp_sql)

    log.info(f'*** 开始分页查询，一共 {len(select_sql_ls)} 页')

    obj_list = []
    threadPool = ThreadPoolExecutor(max_workers=25)
    start_time = time.perf_counter()

    init_file()

    for sel_sql in select_sql_ls:
        log.info(sel_sql)
        obj = threadPool.submit(exec_task, sel_sql)
        if obj:
            obj_list.append(obj)

    for future in as_completed(obj_list, timeout=60):

        data = future.result()

        if data and len(data) > 0:
            # print(len(data))

            for idx, record in enumerate(data):
                sales_address = operate_reocrd(record)  # 发票开票地(市)
                sales_address = sales_address if sales_address else 'null'

                destin_name = str(record[0]) if record[0] else None  # 行程目的地
                sales_name = str(record[1]) if record[1] else None  # 开票公司
                sales_addressphone = str(record[2]) if record[2] else None  # 开票地址及电话
                sales_bank = str(record[3]) if record[3] else None  # 发票开户行
                finance_travel_id = str(record[4]) if record[4] else None
                origin_name = str(record[5]) if record[5] else 'null'  # 行程出发地(市)
                invo_code = str(record[6]) if record[6] else 'null'  # 发票代码

                start_time2 = time.perf_counter()
                origin_province = match_area.query_belong_province(origin_name)  # 行程出发地(省)
                origin_province = origin_province if origin_province else 'null'
                print('111 origin_province => ', origin_province)

                destin_province = match_area.query_destin_province(invo_code=invo_code,
                                                                   destin_name=destin_name)  # 行程目的地(省)
                destin_province = destin_province if destin_province else 'null'
                print('222 destin_province => ', destin_province)

                # origin_province = 'null'
                # destin_province = 'null'

                record = f'{finance_travel_id},{origin_name},{sales_name},{sales_addressphone},{sales_bank},{sales_address},{origin_province},{destin_province}'
                print(record)
                consumed_time2 = round(time.perf_counter() - start_time2)
                log.info(f'* consumed_time2 => {consumed_time2} sec, idx={idx}')
                print('')

                with open(dest_file, "a", encoding='utf-8') as file:
                    file.write(record)
                    file.write("\n")

    threadPool.shutdown(wait=True)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* 查询耗时 {consumed_time} sec')


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

    show_str = f'{sales_name} , {sales_addressphone} , {sales_bank}, {result_area}'
    print('### operate_reocrd show_str ==> ', show_str)

    return result_area


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
    # test_hdfs.uploadFile2(hdfsDirPath=upload_hdfs_path, localPath=dest_file)

    os._exit(0)  # 无错误退出


if __name__ == "__main__":
    main()
