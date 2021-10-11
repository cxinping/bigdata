# -*- coding: utf-8 -*-

from report.commons.logging import get_logger
from report.commons.connect_kudu import prod_execute_sql, dis_connection
import time
import pandas as pd

log = get_logger(__name__)

def execute_sql():
    sql ="""
    select commodityname from  01_datamart_layer_007_h_cw_df.finance_official_bill where commodityname like '%服装%' or 
commodityname like '%餐费%' or commodityname like '%礼品%' or commodityname like '%服装%'
    """


if __name__ == "__main__":
    pass

"""
select commodityname from  01_datamart_layer_007_h_cw_df.finance_official_bill group by commodityname

"""





