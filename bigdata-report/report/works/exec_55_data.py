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

    sql = f'select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_car_bill where commodityname is not null limit 20000'
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

    rd_df['is_blacklist'] = rd_df.apply(
        lambda rd_df: complex_function(rd_df['commodityname'], blacklist_category_ls, whitelist_category_ls), axis=1)

    rd_df = rd_df[rd_df['is_blacklist'] == 1]
    print(rd_df.head(30))
    print('* len==> ', len(rd_df))


def complex_function(commodityname, blacklist_category_ls, whitelist_category_ls):
    is_blacklist = False
    is_whitelist = False
    flag = 0 # 是黑名单返回1， 是白名单返回0

    if blacklist_category_ls:
        for blacklist_category in blacklist_category_ls:
            #print(blacklist_category)
            if commodityname.find(blacklist_category) > -1:
                is_blacklist = True
                break

    if is_blacklist:
        flag = 1
    else:
        if whitelist_category_ls:
            for whitelist_category in whitelist_category_ls:
                #print(whitelist_category)
                if commodityname.find(whitelist_category) > -1:
                    is_whitelist = True
                    break

            if is_whitelist:
                 flag = 0
            else:
                flag = 1

    return flag


exec_55_data()
