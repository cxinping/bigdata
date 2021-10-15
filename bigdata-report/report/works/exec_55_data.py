# -*- coding: utf-8 -*-
from report.commons.logging import get_logger
from report.commons.connect_kudu import prod_execute_sql, dis_connection
import time
import pandas as pd
from report.commons.db_helper import query_kudu_data
from report.commons.tools import list_of_groups
from report.commons.tools import not_empty

log = get_logger(__name__)

import sys
sys.path.append('/you_filed_algos/app')


def exec_55_data():
    columns_ls = ['finance_travel_id', 'bill_id', 'commodityname']
    columns_str = ",".join(columns_ls)

    sql = f'select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_car_bill where commodityname is not null '
    rd_df = query_kudu_data(sql, columns_ls)

    category_columns_ls = ['blacklist_category', 'whitelist_category']
    category_columns_str = ",".join(category_columns_ls)
    category_sql = f"select {category_columns_str} from 01_datamart_layer_007_h_cw_df.finance_blacklist where classify = '车辆使用费'"
    category_df = query_kudu_data(category_sql, category_columns_ls)

    # 黑名单列表
    blacklist_category_ls = category_df['blacklist_category'].tolist()
    # 白名单列表
    whitelist_category_ls = category_df['whitelist_category'].tolist()

    blacklist_category_ls = list(filter(not_empty, blacklist_category_ls))
    whitelist_category_ls = list(filter(not_empty, whitelist_category_ls))

    rd_df['is_blacklist'] = rd_df.apply(lambda rd_df: complex_function(rd_df['commodityname'], blacklist_category_ls, whitelist_category_ls), axis=1)

    # rd_df = rd_df[rd_df['is_blacklist'] == 1]
    print(rd_df.head(500))
    print('* len==> ', len(rd_df))

    # dest_file = "/you_filed_algos/prod_kudu_data/check_55_data.txt"
    # for index, row in rd_df.iterrows():
    #     finance_travel_id = row['finance_travel_id']
    #     commodityname = row['commodityname']
    #
    #     record = f'{finance_travel_id},{commodityname}'
    #     with open(dest_file, "a", encoding='utf-8') as file:
    #         file.write(record)
    #         file.write("\n")

    # finance_travel_id_ls = rd_df['finance_travel_id'].tolist()
    # exec_sql(finance_travel_id_ls)


def complex_function(commodityname, blacklist_category_ls, whitelist_category_ls):
    is_blacklist = False
    is_whitelist = False
    flag = 0  # 是黑名单返回1， 是白名单返回0

    if blacklist_category_ls:
        for blacklist_category in blacklist_category_ls:
            # print(blacklist_category)
            if commodityname.find(blacklist_category) > -1:
                is_blacklist = True
                break

    if is_blacklist:
        flag = 1
    else:
        if whitelist_category_ls:
            for whitelist_category in whitelist_category_ls:
                # print(whitelist_category)
                if commodityname.find(whitelist_category) > -1:
                    is_whitelist = True
                    break

            if is_whitelist:
                flag = 0
            else:
                flag = 1

    return flag


def exec_sql(finance_travel_id_ls):
    print('exec_sql ==> ', len(finance_travel_id_ls))

    if finance_travel_id_ls and len(finance_travel_id_ls) > 0:
        group_ls = list_of_groups(finance_travel_id_ls, 1000)
        # print(len(group_ls), group_ls)

        condition_sql = ''
        in_codition = 'finance_travel_id IN {temp}'

        for idx, group in enumerate(group_ls):
            temp = in_codition.format(temp=str(tuple(group)))
            if idx == 0:
                condition_sql = temp
            else:
                condition_sql = condition_sql + ' OR ' + temp

    # print(condition_sql)
    sql = """
    UPSERT into analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets
    select     
    bill_id, 
    '55' as unusual_id,
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
    0 as meeting_amount from 01_datamart_layer_007_h_cw_df.finance_car_bill 
    WHERE {condition_sql}
    """.format(condition_sql=condition_sql).replace('\n', '').replace('\r', '').strip()
    # print(sql)
    start_time = time.perf_counter()
    prod_execute_sql(conn_type='test', sqltype='insert', sql=sql)
    consumed_time = round(time.perf_counter() - start_time)
    print(f'*** 执行SQL耗时 {consumed_time} sec')


exec_55_data()
print('-- ok --')
