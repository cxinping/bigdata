# -*- coding: utf-8 -*-
from report.commons.connect_kudu2 import prod_execute_sql, dis_connection
from report.commons.settings import CONN_TYPE

"""

处理KUDUde历史数据

"""


def del_history_exception_data():
    sql = "delete from analytic_layer_zbyy_cwyy_014_cwzbbg.finance_all_targets where unusual_id in ('12' ,'13', '14', '34', '49' ) "
    print(sql)
    prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)


def demo1():
    sql1 = "update 01_datamart_layer_007_h_cw_df.finance_unusual set isalgorithm='2' where unusual_id = '12'"
    # prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql1)

    sql2 = f"""
    UPDATE 01_datamart_layer_007_h_cw_df.finance_shell_daily SET task_status="cancel", unusual_infor="系统重启，取消正在执行的执行检查点任务" WHERE task_status="doing"
    """
    # print(sql2)
    # prod_execute_sql(conn_type='prod', sqltype='insert', sql=sql2)

    sql3 = "select * from  01_datamart_layer_007_h_cw_df.finance_unusual where unusual_id = '12' "
    records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sql3)
    for record in records:
        print(record)


if __name__ == '__main__':
    del_history_exception_data()
    #demo1()
    print('--- ok ---')