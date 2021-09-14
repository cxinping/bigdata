# -*- coding: utf-8 -*-
"""
办公费异常检查
@author: WangShuo

"""

from report.commons.logging import get_logger
from report.commons.connect_kudu import prod_execute_sql, dis_connection
from report.commons.tools import match_address
import time
import json
import os

log = get_logger(__name__)


def check_24_invoice():
    start_time = time.perf_counter()
    sql = """

UPSERT into 01_datamart_layer_007_h_cw_df.finance_all_targets 
SELECT bill_id, 
'24' as unusual_id,
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
'会议费',
met_money
from 01_datamart_layer_007_h_cw_df.finance_meeting_bill  
where  billingdate is not null and met_bgdate is not null and met_endate is not null
and (unix_timestamp(billingdate, 'yyyy-MM-dd HH:mm:ss')< unix_timestamp(met_bgdate,'yyyyMMdd')
or unix_timestamp(billingdate, 'yyyy-MM-dd HH:mm:ss')> unix_timestamp(met_endate,'yyyyMMdd'))
    """
    prod_execute_sql(sqltype='insert', sql=sql)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* check_24_invoice SQL耗时 {consumed_time} sec')
    dis_connection()


def check_25_meeting_address():
    columns_ls = ['meet_addr', 'sales_name', 'sales_addressphone', 'sales_bank', 'bill_id']
    columns_str = ",".join(columns_ls)

    sql = """
    select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_meeting_bill where meet_addr is not null and (sales_name is not null and sales_addressphone is not null and sales_bank is not null )  
    """.format(columns_str=columns_str)

    start_time = time.perf_counter()
    select_sql_ls = []
    select_sql_ls.append(sql)
    query_data = []
    for sel_sql in select_sql_ls:
        data = prod_execute_sql(sqltype='select', sql=sel_sql)
        if data:
            query_data.extend(data)

    print('*** 查询记录数量 => ', len(query_data))

    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* 查询耗时 {consumed_time} sec')

    match_bill_id_ls = []
    for data in query_data:
        meet_addr = str(data[0])
        sales_name = str(data[1])
        sales_addressphone = str(data[2])
        sales_bank = str(data[3])
        bill_id = str(data[4])

        # print('111 check ', meet_addr, sales_name, sales_addressphone, sales_bank)

        is_match = False
        if sales_name != 'None' or sales_addressphone != 'None' or sales_bank != 'None':
            # sales_name 只匹配市和县
            if sales_name != 'None':
                sales_name_city = match_address(place=sales_name, key='市')
                if sales_name_city != None:
                    if sales_name_city.find(meet_addr) > -1 or meet_addr.find(sales_name_city) > -1:
                        is_match = True
                        continue

                sales_name_city = match_address(place=sales_name, key='县')
                if sales_name_city != None:
                    if sales_name_city.find(meet_addr) > -1 or meet_addr.find(sales_name_city) > -1:
                        is_match = True
                        continue

            # sales_addressphone 只匹配市和县
            if sales_addressphone != 'None':
                sales_addressphone_city = match_address(place=sales_addressphone, key='市')
                if sales_addressphone_city != None:
                    if sales_addressphone_city.find(meet_addr) > -1 or meet_addr.find(sales_addressphone_city) > -1:
                        is_match = True
                        continue

                sales_addressphone_city = match_address(place=sales_addressphone, key='县')
                if sales_addressphone_city != None:
                    if sales_addressphone_city.find(meet_addr) > -1 or meet_addr.find(sales_addressphone_city) > -1:
                        is_match = True
                        continue

            # sales_bank 只匹配市和县
            if sales_bank != 'None':
                sales_bank_city = match_address(place=sales_bank, key='市')
                if sales_bank_city != None:
                    if sales_bank_city.find(meet_addr) > -1 or meet_addr.find(sales_bank_city) > -1:
                        is_match = True
                        continue

                sales_bank_city = match_address(place=sales_bank, key='县')
                if sales_bank_city != None:
                    if sales_bank_city.find(meet_addr) > -1 or meet_addr.find(sales_bank_city) > -1:
                        is_match = True
                        continue

            if is_match == False:
                match_bill_id_ls.append("'" + bill_id + "'")

    if len(match_bill_id_ls) > 0:
        columns_str = ",".join(match_bill_id_ls)

        sql = f"""
    UPSERT into 01_datamart_layer_007_h_cw_df.finance_all_targets 
    SELECT bill_id, 
    '25' as unusual_id,
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
    '会议费',
    met_money
    from 01_datamart_layer_007_h_cw_df.finance_meeting_bill where bill_id in ({columns_str})        
        """
    start_time = time.perf_counter()
    prod_execute_sql(sqltype='insert', sql=sql)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* 执行 SQL 耗时 {consumed_time} sec')
    dis_connection()


def check_27_consistent_amount():
    start_time = time.perf_counter()
    sql = """
    UPSERT into 01_datamart_layer_007_h_cw_df.finance_all_targets 
    SELECT bill_id, 
    '27' as unusual_id,
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
    '会议费',
    met_money
    from 01_datamart_layer_007_h_cw_df.finance_meeting_bill  
    where check_amount > jzpz
        """
    prod_execute_sql(sqltype='insert', sql=sql)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* check_27_consistent_amount SQL耗时 {consumed_time} sec')
    dis_connection()


def check_30_apply_data():
    start_time = time.perf_counter()
    sql = """
     UPSERT into 01_datamart_layer_007_h_cw_df.finance_all_targets 
    SELECT bill_id, 
    '30' as unusual_id,
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
    '会议费',
    met_money
    from 01_datamart_layer_007_h_cw_df.finance_meeting_bill    
    where apply_id = '' or base_apply_date > met_bgdate 
            """

    prod_execute_sql(sqltype='insert', sql=sql)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* check_30_apply_data SQL耗时 {consumed_time} sec')
    dis_connection()


def check_39_reimburse():
    start_time = time.perf_counter()
    sql = """
      UPSERT into 01_datamart_layer_007_h_cw_df.finance_all_targets 
     SELECT bill_id, 
     '39' as unusual_id,
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
     '会议费',
     met_money
     from 01_datamart_layer_007_h_cw_df.finance_meeting_bill    
     where bill_id in (     
            select a.bill_id  from
                (select bill_id, (unix_timestamp(bill_apply_date, 'yyyyMMdd')-unix_timestamp(met_endate, 'yyyyMMdd'))/ (60 * 60 * 24) as diff_date 
                from  01_datamart_layer_007_h_cw_df.finance_meeting_bill where bill_apply_date > met_endate)a, (
                select standard_value from 01_datamart_layer_007_h_cw_df.finance_standard where unusual_id='39')b
                where a.diff_date > b.standard_value  
     )
             """

    # print(sql)

    prod_execute_sql(sqltype='insert', sql=sql)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* check_39_reimburse SQL耗时 {consumed_time} sec')
    dis_connection()


def main():
    # 需求 24 done
    # check_24_invoice()

    # 需求25 done
    # check_25_meeting_address()

    # 需求 27 done
    # check_27_consistent_amount()

    # 需求30 done
    # check_30_apply_data()

    # 需求39 done
    check_39_reimburse()

    pass


if __name__ == "__main__":
    main()
