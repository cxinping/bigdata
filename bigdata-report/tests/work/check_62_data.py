# -*- coding: utf-8 -*-
from report.commons.db_helper import query_kudu_data
from report.commons.connect_kudu2 import prod_execute_sql

from report.commons.logging import get_logger

log = get_logger(__name__)

import sys

sys.path.append('/you_filed_algos/app')


def check_62_data():
    """
    检查同一人频繁申请报销燃油费用的情况

    """

    columns_ls = ['finance_travel_id', 'bill_id']
    columns_str = ",".join(columns_ls)

    sql = f'select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_car_bill  '
    rd_df = query_kudu_data(sql, columns_ls)
    print(rd_df.head(5))


check_62_data()
