# -*- coding: utf-8 -*-
from report.commons.logging import get_logger
from report.commons.connect_kudu import prod_execute_sql, dis_connection
import time
import pandas as pd

log = get_logger(__name__)

def check_13_data():
    """
    关注相同或临近期间，同一费用发生地，不同人员或企业的住宿费偏离平均值或者大多数人费用分布区间的情况。
    :return:
    """
    columns_ls = ['bill_id', 'destin_name' , 'check_amount']
    columns_str = ",".join(columns_ls)
    sql = 'select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill where destin_name is not null'.format(
        columns_str=columns_str)

    count_sql = 'select count(a.bill_id) from ({sql}) a'.format(sql=sql)
    log.info(count_sql)
    records = prod_execute_sql(conn_type='test', sqltype='select', sql=count_sql)
    count_records = records[0][0]
    log.info(f'* count_records ==> {count_records}')

    records = prod_execute_sql(conn_type='test', sqltype='select', sql=sql)
    print('len(records) ==> ' , len(records))






check_13_data()
print('--- ok ---')
