# -*- coding: utf-8 -*-
from report.commons.logging import get_logger
from report.commons.connect_kudu import prod_execute_sql, dis_connection
import time
import pandas as pd
from report.commons.db_helper import query_kudu_data

log = get_logger(__name__)

import sys
sys.path.append('/you_filed_algos/app')


def check_34_data():
    columns_ls = ['finance_travel_id', 'bill_id', 'meet_lvl_name' , 'met_money']
    columns_str = ",".join(columns_ls)

    sql2 = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_meeting_bill where meet_lvl_name is not null and meet_lvl_name !='不适用' and met_money is not null limit 10".format(
        columns_str=columns_str)
    sql = f"""
    select 
     {columns_str},
    datediff(
        concat(left(met_endate,4),'-',substr(met_endate,5,2),'-',right(met_endate,2)),
        concat(left(met_bgdate,4),'-',substr(met_bgdate,5,2),'-',right(met_bgdate,2))
    )+1 as meeting_time   
     from 01_datamart_layer_007_h_cw_df.finance_meeting_bill 
     where meet_lvl_name is not null and meet_lvl_name !='不适用' and met_money is not null limit 10
    """
    print(sql)
    columns_ls.append('meeting_time')
    print(columns_ls)
    rd_df = query_kudu_data(sql, columns_ls)
    print(rd_df.head(10))
    print(rd_df.dtypes)
    #print(rd_df.describe())
    print(len(rd_df))
    print('*' * 50)

    #group_rd_df = rd_df.groupby('meet_lvl_name')
    group_rd_df = rd_df['met_money'].groupby(rd_df['meet_lvl_name'])
    #print(group_rd_df.head(10))

    #std_df = group_rd_df.std()
    #print(std_df)

    #print(type(std_df))
    type_1_meeting = None   # 一类会议
    type_2_meeting = None   # 二类会议
    type_3_meeting = None   # 三类会议
    type_4_meeting = None   # 四类会议

    # for value in std_df.items():
    #     print(value)





check_34_data()



