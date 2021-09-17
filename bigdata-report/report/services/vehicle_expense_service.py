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
         UPSERT into analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets
        SELECT bill_id, 
        '54' as unusual_id,
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
        base_beg_date  as beg_date,
        base_end_date  as end_date,
        '' as emp_name,
        '' as emp_code,
        0 as jour_amount,
        0 as accomm_amount,
        0 as subsidy_amount,
        0 as other_amount,
        check_amount,
        jzpz,
        '车辆使用费',
        0 as meeting_amount
        from 01_datamart_layer_007_h_cw_df.finance_car_bill 
    where billingdate is not null and substr(billingdate,1,4)<>cast(year(now()) as string)
                """

    prod_execute_sql(sqltype='insert', sql=sql)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* check_54_invoice SQL耗时 {consumed_time} sec')
    dis_connection()


def check_56_consistent_amount():
    start_time = time.perf_counter()
    sql = """
             UPSERT into analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets
        SELECT bill_id, 
        '56' as unusual_id,
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
        base_beg_date  as beg_date,
        base_end_date  as end_date,
        '' as emp_name,
        '' as emp_code,
        0 as jour_amount,
        0 as accomm_amount,
        0 as subsidy_amount,
        0 as other_amount,
        check_amount,
        jzpz,
        '车辆使用费',
        0 as meeting_amount
    from 01_datamart_layer_007_h_cw_df.finance_car_bill  
    where check_amount > jzpz
        """
    prod_execute_sql(sqltype='insert', sql=sql)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* check_56_consistent_amount SQL耗时 {consumed_time} sec')
    dis_connection()


def check_65_reimburse():
    start_time = time.perf_counter()
    sql = """
            UPSERT into analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets
        SELECT bill_id, 
        '65' as unusual_id,
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
        base_beg_date  as beg_date,
        base_end_date  as end_date,
        '' as emp_name,
        '' as emp_code,
        0 as jour_amount,
        0 as accomm_amount,
        0 as subsidy_amount,
        0 as other_amount,
        check_amount,
        jzpz,
        '车辆使用费',
        0 as meeting_amount
    from 01_datamart_layer_007_h_cw_df.finance_car_bill  
    where bill_id in (
        select a.bill_id from
            (select bill_id, (unix_timestamp(bill_apply_date, 'yyyyMMdd')-unix_timestamp(tb_times, 'yyyyMMdd'))/ (60 * 60 * 24) as diff_date 
            from  01_datamart_layer_007_h_cw_df.finance_car_bill where bill_apply_date > tb_times)a, (
            select standard_value from 01_datamart_layer_007_h_cw_df.finance_standard where unusual_id='65')b
            where a.diff_date > b.standard_value      
    )         
        """
    prod_execute_sql(sqltype='insert', sql=sql)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* check_65_reimburse SQL耗时 {consumed_time} sec')
    dis_connection()

def check_64_credit():
    start_time = time.perf_counter()
    sql = """
    UPSERT into analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets
        SELECT bill_id, 
        '64' as unusual_id,
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
        base_beg_date  as beg_date,
        base_end_date  as end_date,
        '' as emp_name,
        '' as emp_code,
        0 as jour_amount,
        0 as accomm_amount,
        0 as subsidy_amount,
        0 as other_amount,
        check_amount,
        jzpz,
        '车辆使用费',
        0 as meeting_amount
    from 01_datamart_layer_007_h_cw_df.finance_car_bill  
    where bill_id in (
       select bill_id from 01_datamart_layer_007_h_cw_df.finance_car_bill where account_period is not null 
        and arrivedtimes is not null
        and cast(concat(substr(account_period,1,4),substr(account_period,6,2)) as int)> cast(replace(substr(arrivedtimes,1,7),'-','')  as int)     
    )         
        """
    prod_execute_sql(sqltype='insert', sql=sql)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* check_64_credit SQL耗时 {consumed_time} sec')
    dis_connection()


def check_66_approve():
    start_time = time.perf_counter()
    sql = """
    with standard as (
        select standard_value from 01_datamart_layer_007_h_cw_df.finance_standard where unusual_id='66'
    )

     UPSERT into analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets
        SELECT bill_id, 
        '66' as unusual_id,
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
        base_beg_date  as beg_date,
        base_end_date  as end_date,
        '' as emp_name,
        '' as emp_code,
        0 as jour_amount,
        0 as accomm_amount,
        0 as subsidy_amount,
        0 as other_amount,
        check_amount,
        jzpz,
        '车辆使用费',
        0 as meeting_amount
    from 01_datamart_layer_007_h_cw_df.finance_car_bill  
    where bill_id in (
       select bill_id from 
        01_datamart_layer_007_h_cw_df.finance_car_bill,standard
        where 
        arrivedtimes is not null
        and 
        tb_times is not null
        and
        ((cast(replace(substr(arrivedtimes,1,7),'-','') as int)-cast(replace(substr(tb_times,1,7),'-','')  as int))-(select avg(cast(replace(substr(arrivedtimes,1,7),'-','')  as int)-cast(replace(substr(tb_times,1,7),'-','')  as int)) avgt from 
        01_datamart_layer_007_h_cw_df.finance_car_bill))>standard.standard_value
    )         
        """
    prod_execute_sql(sqltype='insert', sql=sql)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* check_66_approve SQL耗时 {consumed_time} sec')
    dis_connection()

def main():
    # 需求 54 done
    #check_54_invoice()

    # 需求 56 done 未测试
    #check_56_consistent_amount()

    # 需求 64
    check_64_credit()

    # 需求 65 done 未测试
    #check_65_reimburse()

    # 需求 66 done
    #check_66_approve()

    pass


if __name__ == "__main__":
    main()
