# -*- coding: utf-8 -*-
"""
车辆使用费异常检查
@author: WangShuo

"""

from report.commons.logging import get_logger
from report.commons.connect_kudu import prod_execute_sql, dis_connection
from report.commons.tools import match_address
import time
import json
import os

log = get_logger(__name__)

def check_54_invoice():
    start_time = time.perf_counter()
    sql = """
         UPSERT into 01_datamart_layer_007_h_cw_df.finance_all_targets 
        SELECT bill_id, 
        '41' as unusual_id,
        company_code,
        account_period,
        account_item,
        finance_number,
        cost_center,
        profit_center,
        '' as cart_head,
        bill_code,
        ''   as  origin_city,
        ''  as destin_city,
        met_bgdate  as beg_date,
        met_endate  as end_date,
        '' as emp_name,
        '' as emp_code,
        0 as jour_amount,
        0 as accomm_amount,
        0 as subsidy_amount,
        0 as other_amount,
        check_amount,
        jzpz,
        '车辆使用费',
        met_money
        from 01_datamart_layer_007_h_cw_df.finance_car_bill 
    where billingdate is not null and substr(billingdate,1,4)<>cast(year(now()) as string)
                """

    prod_execute_sql(sqltype='insert', sql=sql)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* check_54_invoice SQL耗时 {consumed_time} sec')
    dis_connection()


def main():

    #需求 54 未测试
    check_54_invoice()
    pass

if __name__ == "__main__":
    main()
