# -*- coding: utf-8 -*-
from report.commons.tools import create_uuid
from report.commons.settings import CONN_TYPE
from report.commons.logging import get_logger
from report.commons.connect_kudu2 import prod_execute_sql

log = get_logger(__name__)

"""
数据流程处理

"""


def insert_temp_performance_bill(order_number, sign_status, performance_sql):
    performance_id = create_uuid()
    try:
        # log.info('*** insert_finance_shell_daily ***')
        sql = f"""
        insert into 01_datamart_layer_007_h_cw_df.temp_performance_bill(performance_id, order_number, sign_status, performance_sql) 
        values("{performance_id}", "{order_number}", "{sign_status}", "{performance_sql}" )
        """.replace('\n', '').replace('\r', '').strip()
        log.info(sql)
        prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)
        return performance_id
    except Exception as e:
        print(e)
        raise RuntimeError(e)


def update_temp_performance_bill(performance_id, order_number, sign_status, performance_sql):
    try:
        sql = f"""
        UPDATE 01_datamart_layer_007_h_cw_df.temp_performance_bill SET order_number="{order_number}", sign_status="{sign_status}",performance_sql="{performance_sql}" WHERE performance_id="{performance_id}"
        """.replace('\n', '').replace('\r', '').strip()
        log.info(sql)
        prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)
        return performance_id
    except Exception as e:
        print(e)
        raise RuntimeError(e)


def pagination_temp_performance_bill_records():
    """
    分页查询绩效临时表的记录

    :return:
    """
    columns_ls = ['performance_id', 'order_number', 'sign_status', 'performance_sql']
    columns_str = ",".join(columns_ls)
    sql = f"SELECT {columns_str} FROM 01_datamart_layer_007_h_cw_df.temp_performance_bill "

    count_sql = 'SELECT count(1) FROM ({sql}) a'.format(sql=sql)
    log.info(count_sql)
    records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=count_sql)
    count_records = records[0][0]
    # print('* count_records => ', count_records)

    ###### 拼装查询SQL
    where_sql = 'WHERE '
    condition_sql = ''

    where_sql = where_sql + ' 1=1'

    order_sql = ' ORDER BY order_number ASC '
    sql = sql + where_sql + order_sql

    return count_records, sql, columns_ls
