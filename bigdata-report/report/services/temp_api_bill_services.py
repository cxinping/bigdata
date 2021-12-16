# -*- coding: utf-8 -*-

from report.commons.connect_kudu2 import prod_execute_sql
from report.commons.settings import CONN_TYPE
from report.commons.logging import get_logger
from report.commons.tools import get_current_time
from report.services.common_services import (insert_finance_shell_daily, update_finance_shell_daily)

log = get_logger(__name__)


# CONN_TYPE = 'test'


def exec_temp_api_bill_sql(target_classify):
    """
    执行临时表API表
    :param target_classify: 差旅费、会议费、办公费、车辆使用费
    :return:
    """

    try:
        sql = f"""
            select tem_api_id,target_classify,api_sql  from  01_datamart_layer_007_h_cw_df.temp_api_bill
            where target_classify="{target_classify}" order by tem_api_id asc
            """
        temp_api_sql_records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sql)
        log.info(f'需要执行 {temp_api_sql_records} 条SQL')
        daily_start_date = get_current_time()
        daily_id = insert_finance_shell_daily(daily_status='ok', daily_start_date=daily_start_date,
                                              daily_end_date='', unusual_point='',
                                              daily_source='sql',
                                              operate_desc=f'正在执行临时表API中类型为{target_classify}的SQL', unusual_infor='',
                                              task_status='doing', daily_type='数据处理')

        for idx, record in enumerate(temp_api_sql_records):
            api_sql = record[2]
            # log.info(api_sql)
            prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=api_sql)
            log.info(f'------------------ 执行成功第{idx + 1}条临时表的SQL,共有{len(temp_api_sql_records)}条SQL ------------------')

        operate_desc = f'成功执行临时表API中类型为{target_classify}的SQL'
        daily_end_date = get_current_time()
        update_finance_shell_daily(daily_id, daily_end_date, task_status='done', operate_desc=operate_desc)
    except Exception as e:
        print(e)
        error_info = str(e)
        daily_end_date = get_current_time()
        update_finance_shell_daily(daily_id, daily_end_date, task_status='error', operate_desc=error_info)
