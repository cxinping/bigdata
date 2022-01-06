# -*- coding: utf-8 -*-

from report.commons.connect_kudu2 import prod_execute_sql
from report.commons.settings import CONN_TYPE
from report.commons.logging import get_logger
from report.commons.tools import get_current_time
from report.services.common_services import (insert_finance_shell_daily, update_finance_shell_daily)
from report.commons.tools import create_uuid, list_of_groups

log = get_logger(__name__)


def exec_temp_api_bill_sql_by_ids(tem_api_ids):
    """
    执行临时表API表,
    :param tem_api_ids : 临时表的主键ids
    :return:
    """

    try:
        sql = "SELECT api_sql,tem_api_id FROM 01_datamart_layer_007_h_cw_df.temp_api_bill WHERE "

        condition_sql = ''
        in_codition = 'tem_api_id IN {temp}'

        if tem_api_ids and len(tem_api_ids) > 0:
            group_ls = list_of_groups(tem_api_ids, 1000)

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
            log.info(sql)
            records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sql)
            for record in records:
                api_sql = str(record[0])
                tem_api_id = str(record[1])

                prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=api_sql)
                log.info(f'成功执行了tem_api_id为{tem_api_id}的临时表的SQL, 共有{len(records)}条SQL ')

    except Exception as e:
        #print(e)
        error_info = str(e)

        raise RuntimeError(e)


def exec_temp_api_bill_sql_by_target(target_classify):
    """
    执行临时表API表,
    :param target_classify : 目标分类，主要包括：差旅费、会议费、办公费、车辆使用费
    :return:
    """

    try:
        log.info(f'*** exec_temp_api_bill_sql_by_target, 执行 {target_classify} 的绩效SQL ***')
        sql = f"""
            select tem_api_id,target_classify,api_sql  from  01_datamart_layer_007_h_cw_df.temp_api_bill
            where target_classify="{target_classify}" order by order_number asc
            """
        temp_api_sql_records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sql)
        log.info(f'需要执行 {len(temp_api_sql_records)} 条SQL')
        daily_start_date = get_current_time()
        daily_id = insert_finance_shell_daily(daily_status='ok', daily_start_date=daily_start_date,
                                              daily_end_date='', unusual_point='',
                                              daily_source='sql',
                                              operate_desc=f'正在执行临时表API中类型为{target_classify}的SQL', unusual_infor='',
                                              task_status='doing', daily_type='数据处理')

        for idx, record in enumerate(temp_api_sql_records):
            tem_api_id = str(record[0])
            api_sql = str(record[2])
            # log.info(api_sql)
            prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=api_sql)
            log.info(f'target_classify={target_classify},执行成功tem_api_id为 {tem_api_id} 的临时表的SQL,共有{len(temp_api_sql_records)}条SQL')

        operate_desc = f'成功执行临时表API中类型为{target_classify}的SQL'
        daily_end_date = get_current_time()
        update_finance_shell_daily(daily_id, daily_end_date, task_status='done', operate_desc=operate_desc)
    except Exception as e:
        #print(e)
        error_info = str(e)
        daily_end_date = get_current_time()
        update_finance_shell_daily(daily_id, daily_end_date, task_status='error', operate_desc=error_info)
        raise RuntimeError(e)


def insert_temp_api_bill(order_number, target_classify, api_sql):
    tem_api_id = create_uuid()
    try:
        # log.info('*** insert_temp_api_bill ***')
        sql = f"""
        insert into 01_datamart_layer_007_h_cw_df.temp_api_bill(tem_api_id, order_number, target_classify, api_sql ) 
        values("{tem_api_id}", "{order_number}", "{target_classify}","{api_sql}" )
        """
        log.info(sql)
        prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)
        return tem_api_id
    except Exception as e:
        print(e)
        raise RuntimeError(e)


def update_temp_api_bill(tem_api_id, order_number, target_classify, api_sql):
    try:
        sql = f"""
        UPDATE 01_datamart_layer_007_h_cw_df.temp_api_bill SET order_number="{order_number}", target_classify="{target_classify}",api_sql="{api_sql}" WHERE tem_api_id="{tem_api_id}"
        """
        log.info(sql)
        prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)
        return tem_api_id
    except Exception as e:
        print(e)
        raise RuntimeError(e)


def delete_temp_api_bill(tem_api_ids):
    try:
        sql = "DELETE FROM 01_datamart_layer_007_h_cw_df.temp_api_bill WHERE "

        condition_sql = ''
        in_codition = 'tem_api_id IN {temp}'

        if tem_api_ids and len(tem_api_ids) > 0:
            group_ls = list_of_groups(tem_api_ids, 1000)

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


def pagination_temp_api_bill_records():
    """
    分页查询临时表的记录

    :return:
    """
    columns_ls = ['tem_api_id', 'order_number', 'target_classify', 'api_sql']
    columns_str = ",".join(columns_ls)
    sql = f"SELECT {columns_str} FROM 01_datamart_layer_007_h_cw_df.temp_api_bill "

    count_sql = 'SELECT count(1) FROM ({sql}) a'.format(sql=sql)
    log.info(count_sql)
    records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=count_sql)
    count_records = records[0][0]
    # print('* count_records => ', count_records)

    ###### 拼装查询SQL
    where_sql = 'WHERE '
    condition_sql = ''

    where_sql = where_sql + ' 1=1 '

    order_sql = ' ORDER BY order_number ASC '
    sql = sql + where_sql + order_sql

    return count_records, sql, columns_ls
