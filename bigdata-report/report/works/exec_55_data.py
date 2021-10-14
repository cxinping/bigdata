# -*- coding: utf-8 -*-
from report.commons.logging import get_logger
from report.commons.connect_kudu import prod_execute_sql, dis_connection
import time
import pandas as pd
from report.commons.db_helper import query_kudu_data
from report.commons.tools import list_of_groups

log = get_logger(__name__)

import sys
sys.path.append('/you_filed_algos/app')


def exec_55_data():
    columns_ls = ['finance_travel_id', 'bill_id', 'commodityname']
    columns_str = ",".join(columns_ls)

    sql = f'select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_car_bill where commodityname is not null limit 10'
    rd_df = query_kudu_data(sql, columns_ls)

    print(rd_df.head(5))
    print(rd_df.dtypes)

    category_columns_ls = ['blacklist_category' , 'whitelist_category']
    category_columns_str = ",".join(category_columns_ls)
    category_sql = f"select {category_columns_str} from 01_datamart_layer_007_h_cw_df.finance_blacklist where classify = '办公费'"
    category_df = query_kudu_data(category_sql, category_columns_ls)

    # 黑名单列表
    blacklist_category_ls = category_df['blacklist_category'].tolist()
    # 白名单列表
    whitelist_category_ls = category_df['whitelist_category'].tolist()






exec_55_data()
