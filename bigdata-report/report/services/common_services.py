# -*- coding: utf-8 -*-
from report.commons.tools import create_uuid
from report.commons.connect_kudu import prod_execute_sql
from report.commons.logging import get_logger


log = get_logger(__name__)



def insert_finance_shell_daily(daily_status, daily_start_date,daily_end_date, unusual_point, daily_source, operate_desc, unusual_infor ):
    daily_id = create_uuid()
    sql = f"""
    insert into 01_datamart_layer_007_h_cw_df.finance_shell_daily(daily_id, daily_status, daily_start_date, daily_end_date, unusual_point, daily_source, operate_desc, unusual_infor) 
    values('{daily_id}', '{daily_status}', '{daily_start_date}', '{daily_end_date}' ,'{unusual_point}', '{daily_source}', '{operate_desc}', '{unusual_infor}' )
    """.replace('\n', '').replace('\r', '').strip()
    prod_execute_sql(conn_type='test', sqltype='insert', sql=sql)


if __name__ == "__main__":
    daily_status  = 'ok'
    daily_start_date  = '2021-10-25 17:05'
    daily_end_date  = '2021-10-25 20:05'
    unusual_point  = '42'
    daily_source = 'sql'
    operate_desc  = 'aaabbbccc'
    unusual_infor = 'ggggggggggggg'

    insert_finance_shell_daily(daily_status, daily_start_date,daily_end_date, unusual_point, daily_source, operate_desc, unusual_infor)
    print('--- ok ---')