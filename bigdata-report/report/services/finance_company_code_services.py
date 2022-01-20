# -*- coding: utf-8 -*-

from report.commons.tools import create_uuid
from report.commons.settings import CONN_TYPE
from report.commons.logging import get_logger
from report.commons.connect_kudu2 import prod_execute_sql
from report.commons.tools import list_of_groups

log = get_logger(__name__)


def insert_finance_company_code(company_name, company_code, company_old_code, iscompany):
    """
    添加单位code表的数据
    :return:
    """
    id = create_uuid()
    try:
        log.info('*** insert_finance_company_code ***')
        sql = f"""
        insert into 01_datamart_layer_007_h_cw_df.finance_company_code(id, company_name, company_code, company_old_code, iscompany) 
        values("{id}", "{company_name}", "{company_code}", "{company_old_code}","{iscompany}"  )
        """.replace('\n', '').replace('\r', '').strip()
        log.info(sql)
        prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)
        return id
    except Exception as e:
        print(e)
        raise RuntimeError(e)


def update_finance_company_code(id, company_name, company_code, company_old_code, iscompany):
    try:
        sql = f"""
        UPDATE 01_datamart_layer_007_h_cw_df.finance_company_code SET company_name="{company_name}", company_code="{company_code}",company_old_code="{company_old_code}",iscompany="{iscompany}" WHERE id="{id}"
        """
        log.info(sql)
        prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)
        return id
    except Exception as e:
        print(e)
        raise RuntimeError(e)


def del_finance_company_code(ids):
    try:
        sql = 'DELETE FROM 01_datamart_layer_007_h_cw_df.finance_company_code WHERE '
        condition_sql = ''
        in_codition = 'id IN {temp}'

        if ids and len(ids) > 0:
            group_ls = list_of_groups(ids, 1000)

            for idx, group in enumerate(group_ls):
                if len(group) == 1:
                    temp = in_codition.format(temp=str('("' + group[0] + '")'))
                else:
                    temp = in_codition.format(temp=str(tuple(group)))

                if idx == 0:
                    condition_sql = temp
                else:
                    condition_sql = condition_sql + ' OR ' + temp

            sql = sql + condition_sql

            # log.info(sql)
            prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)
    except Exception as e:
        print(e)
        raise RuntimeError(e)


def pagination_finance_company_code_records(company_name=None):
    """
    分页查询单位code表的记录

    :return:
    """

    # sql = f"SELECT {columns_str} FROM 01_datamart_layer_007_h_cw_df.finance_company_code "

    # count_sql = 'SELECT count(1) FROM ({sql}) a'.format(sql=sql)
    # log.info(count_sql)
    # records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=count_sql)
    # count_records = records[0][0]
    # print('* count_records => ', count_records)

    ###### 拼装查询SQL
    # where_sql = 'WHERE '
    # condition_sql = ''
    #
    # where_sql = where_sql + ' 1=1 '
    #
    # order_sql = ' ORDER BY id ASC '
    # sql = sql + where_sql + order_sql

    columns_ls = ['id', 'company_name', 'company_code', 'company_old_code', 'iscompany']
    columns_str = ",".join(columns_ls)

    ###### 拼装查询SQL

    where_sql = 'WHERE '

    if company_name is None:
        where_sql = where_sql + f' 1=1  '
    elif company_name:
        where_sql = where_sql + f' company_name like "%{company_name}%"   '

    sql = f"SELECT {columns_str} FROM 01_datamart_layer_007_h_cw_df.finance_company_code "
    order_sql = ' ORDER BY id ASC '
    sql = sql + where_sql + order_sql

    count_sql = 'SELECT count(a.id) FROM ({sql}) a'.format(sql=sql)
    # log.info(count_sql)
    records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=count_sql)
    count_records = records[0][0]

    return count_records, sql, columns_ls
