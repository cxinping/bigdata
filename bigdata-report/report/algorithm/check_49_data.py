# -*- coding: utf-8 -*-

from report.commons.logging import get_logger
from report.commons.connect_kudu import prod_execute_sql, dis_connection
import time
import pandas as pd
from report.commons.db_helper import query_kudu_data

log = get_logger(__name__)

import sys
sys.path.append('/you_filed_algos/app')

def check_49_data():
    columns_ls = ['finance_travel_id', 'bill_id', 'check_amount']
    columns_str = ",".join(columns_ls)

    sql = 'select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_official_bill '.format(
        columns_str=columns_str)
    start_time = time.perf_counter()
    rd_df = query_kudu_data(sql, columns_ls)
    # print(rd_df.head())
    # print(rd_df.dtypes)
    # print('*' * 50)
    # print(rd_df.describe())

    temp = rd_df.describe()[['check_amount']]
    mean_val = temp.at['mean', 'check_amount']  # 平均值
    std_val = temp.at['std', 'check_amount']  # 方差

    result = rd_df[rd_df['check_amount'] > std_val]
    #print(result)

    bill_id_ls = result['bill_id'].tolist()
    exec_sql(bill_id_ls)

    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* 查询耗时 {consumed_time} sec')


def exec_sql(bill_id_ls):
    sql = """
    UPSERT INTO analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets
    SELECT 
    bill_id, 
    '49' as unusual_id,
    company_code,
    account_period,
    account_item,
    finance_number,
    cost_center,
    profit_center,
    '' as cart_head,
    bill_code,
  bill_beg_date,
  bill_end_date,
    ''   as  origin_city,
    ''  as destin_city,
    base_beg_date  as beg_date,
    base_end_date  as end_date,
  apply_emp_name,
    '' as emp_name,
    '' as emp_code,
  '' as company_name,
    0 as jour_amount,
    0 as accomm_amount,
    0 as subsidy_amount,
    0 as other_amount,
    check_amount,
    jzpz,
    '办公费',
    0 as meeting_amount
    FROM 01_datamart_layer_007_h_cw_df.finance_official_bill 
    WHERE bill_id IN {bill_id_ls}
        """.format(bill_id_ls=tuple(bill_id_ls)).replace('\n', '').replace('\r', '').strip()
    #print(sql)
    print(len(bill_id_ls))

    start_time = time.perf_counter()
    #prod_execute_sql(conn_type='test', sqltype='insert', sql=sql)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'*** 执行SQL耗时 {consumed_time} sec')


check_49_data()
print('--- ok ---')
