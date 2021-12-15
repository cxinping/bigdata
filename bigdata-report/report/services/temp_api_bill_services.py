# -*- coding: utf-8 -*-

from report.commons.connect_kudu2 import prod_execute_sql
from report.commons.settings import CONN_TYPE
from report.commons.logging import get_logger

log = get_logger(__name__)

CONN_TYPE = 'test'


def exec_temp_api_bill_sql(target_classify):
    """
    执行临时表API表
    :param target_classify: 差旅费、会议费、办公费、车辆使用费
    :return:
    """
    sql = f"""
    select tem_api_id,target_classify,api_sql  from  01_datamart_layer_007_h_cw_df.temp_api_bill
    where target_classify="{target_classify}" order by tem_api_id asc
    """
    temp_api_sql_records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sql)
    for idx, record in enumerate(temp_api_sql_records):
        api_sql = record[2]
        log.info(api_sql)
        prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=api_sql)
        print(f'------------------ 执行成功第{idx+1}条临时表的SQL ------------------' )


