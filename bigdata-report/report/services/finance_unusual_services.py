# -*- coding: utf-8 -*-
from report.commons.tools import create_uuid
from report.commons.settings import CONN_TYPE
from report.commons.logging import get_logger
from report.commons.connect_kudu2 import prod_execute_sql

log = get_logger(__name__)


def pagination_finance_unusual_records(unusual_point):
    """
    分页查询记录

    :return:
    """

    ###### 拼装查询SQL
    where_sql = 'WHERE '
    condition_sql = ''

    if unusual_point is None:
        where_sql = where_sql + ' 1=1 '
    else:
        where_sql = where_sql + f' unusual_point LIKE "%{unusual_point}%" '

    order_sql = ' ORDER BY unusual_id ASC '

    columns_ls = ['finance_id', 'cost_project', 'unusual_id', 'number_name', 'unusual_point', 'unusual_content',
                  'unusual_shell', 'isalgorithm',
                  'importdate', 'sign_status', 'unusual_level']
    columns_str = ",".join(columns_ls)
    sql = f"SELECT {columns_str} FROM 01_datamart_layer_007_h_cw_df.finance_unusual {where_sql}"

    count_sql = 'SELECT count(1) FROM ({sql}) a'.format(sql=sql)
    #log.info(count_sql)

    records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=count_sql)
    count_records = records[0][0]
    # print('* count_records => ', count_records)
    sql = sql + order_sql

    return count_records, sql, columns_ls

