# -*- coding: utf-8 -*-
import time

# from report.commons.connect_kudu import prod_execute_sql
from report.commons.connect_kudu2 import prod_execute_sql

from report.commons.db_helper import query_kudu_data
from report.commons.logging import get_logger
from report.commons.tools import list_of_groups

log = get_logger(__name__)

import sys

sys.path.append('/you_filed_algos/app')

import pandas as pd

# 设置显示最大列数 与 显示宽度
pd.set_option('display.max_columns',None)
pd.set_option('display.width', 500)

def check_34_data():
    columns_ls = ['finance_meeting_id', 'bill_id', 'meet_lvl_name', 'met_money']
    columns_str = ",".join(columns_ls)

    sql = f"""
    select 
     {columns_str},
     ( met_money / (
        datediff(
            concat(left(met_endate,4),'-',substr(met_endate,5,2),'-',right(met_endate,2)),
            concat(left(met_bgdate,4),'-',substr(met_bgdate,5,2),'-',right(met_bgdate,2))
        )+1 
    )) as per_day_met_money 
     from 01_datamart_layer_007_h_cw_df.finance_meeting_bill 
     where meet_lvl_name is not null and meet_lvl_name !='不适用' and met_money is not null 
    """
    print(sql)
    columns_ls.append('per_day_met_money')  # 每人每天的会议费

    #print(columns_ls)
    rd_df = query_kudu_data(sql, columns_ls)
    print(rd_df.head(10))
    #print(rd_df.dtypes)
    # print(rd_df.describe())
    # print(len(rd_df))
    print('*' * 50)

    group_rd_df = rd_df['per_day_met_money'].groupby(rd_df['meet_lvl_name'])
    std_df = group_rd_df.std()

    # print(type(std_df))
    type_1_meeting_std = None  # 一类会议对应的会议费方差
    type_2_meeting_std = None  # 二类会议对应的会议费方差
    type_3_meeting_std = None  # 三类会议对应的会议费方差
    type_4_meeting_std = None  # 四类会议对应的会议费方差

    for value in std_df.items():
        # print(value, value[1])
        if value[0] == '一类会议':
            type_1_meeting_std = value[1]
        elif value[0] == '二类会议':
            type_2_meeting_std = value[1]
        elif value[0] == '三类会议':
            type_3_meeting_std = value[1]
        elif value[0] == '四类会议':
            type_4_meeting_std = value[1]

    print('type_1_meeting_std=', type_1_meeting_std)
    print('type_2_meeting_std=', type_2_meeting_std)
    print('type_3_meeting_std=', type_3_meeting_std)
    print('type_4_meeting_std=', type_4_meeting_std)

    # 过滤dataframe ，按照 meet_lvl_name对应的方差，
    rd_df = rd_df[((rd_df.meet_lvl_name == '一类会议') & (rd_df.per_day_met_money > type_1_meeting_std))
                  | ((rd_df.meet_lvl_name == '二类会议') & (rd_df.per_day_met_money > type_2_meeting_std))
                  | ((rd_df.meet_lvl_name == '三类会议') & (rd_df.per_day_met_money > type_3_meeting_std))
                  | ((rd_df.meet_lvl_name == '四类会议') & (rd_df.per_day_met_money > type_4_meeting_std))]

    print(rd_df.head(10))
    bill_id_ls = rd_df['bill_id'].tolist()

    if len(bill_id_ls) > 0:
        #exec_sql(bill_id_ls)  #
        pass
    else:
        print('* bill_id_ls length is 0 ')

    print('--- check_34 has been completed ')


def exec_sql(bill_id_ls):
    print('exec_sql ==> ', len(bill_id_ls))

    if bill_id_ls and len(bill_id_ls) > 0:
        group_ls = list_of_groups(bill_id_ls, 1000)
        # print(len(group_ls), group_ls)

        condition_sql = ''
        in_codition = 'bill_id IN {temp}'

        for idx, group in enumerate(group_ls):
            temp = in_codition.format(temp=str(tuple(group)))
            if idx == 0:
                condition_sql = temp
            else:
                condition_sql = condition_sql + ' OR ' + temp

    sql = """
    UPSERT INTO analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets
    SELECT 
    bill_id, 
    '34' as unusual_id,
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
    '会议费',
    0 as meeting_amount
    FROM 01_datamart_layer_007_h_cw_df.finance_meeting_bill 
    WHERE {condition_sql}
        """.format(condition_sql=condition_sql)#.replace('\n', '').replace('\r', '').strip()
    print(sql)
    start_time = time.perf_counter()
    prod_execute_sql(conn_type='test', sqltype='insert', sql=sql)
    consumed_time = round(time.perf_counter() - start_time)
    print(f'*** 执行SQL耗时 {consumed_time} sec')


check_34_data()
