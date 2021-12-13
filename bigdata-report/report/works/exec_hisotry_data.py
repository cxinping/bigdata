# -*- coding: utf-8 -*-
from report.commons.connect_kudu2 import prod_execute_sql, dis_connection
from report.commons.settings import CONN_TYPE
from report.commons.logging import get_logger
import threading
import time

log = get_logger(__name__)

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
    sql2 = '01_datamart_layer_007_h_cw_df.finance_shell_daily'
    # print(sql2)
    # prod_execute_sql(conn_type='prod', sqltype='insert', sql=sql2)

    # sql3 = "select * from  01_datamart_layer_007_h_cw_df.finance_unusual where unusual_id = '02' "
    # sql3 = "select count(1) from analytic_layer_zbyy_cwyy_014_cwzbbg.finance_all_targets where unusual_id =  '01' "
    sql3 = 'select * from 01_datamart_layer_007_h_cw_df.finance_shell_daily where unusual_point="02" '
    print(sql3)
    records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sql3)
    for record in records:
        print(record)


def demo2():
    sql = """
      delete from 01_datamart_layer_007_h_cw_df.finance_shell_daily where unusual_point="01"
    """
    sql2 = 'delete from 01_datamart_layer_007_h_cw_df.finance_shell_daily'

    sql3 = """
    
    """

    prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql2)


def exec_sql():
    sql = "select unusual_id, unusual_shell from  01_datamart_layer_007_h_cw_df.finance_unusual where isalgorithm='1' order by unusual_id"
    records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sql)
    for record in records:
        unusual_id = record[0]
        unusual_shell = record[1]

        """
        检查点 01 不用执行
        检查点 02 执行时间长
        """
        if unusual_id not in ['01', '02', '05', '06', '07', '11', '18', '31', '36', '44', '48', '57', '59',
                              '61'] :  # '02','03', '04'
            #log.info(f'开始执行 检查点{unusual_id} 的SQL')
            #log.info(unusual_shell)

            try:
                if unusual_shell is not None and unusual_shell != 'None':
                    prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=unusual_shell)
                    log.info(f'成功执行 检查点{unusual_id} 的SQL')
            except Exception as e:
                print(e)
                log.info(f'error 执行检查点{unusual_id} 的SQL失败')


if __name__ == '__main__':
    # del_history_exception_data()
    # demo1()
    exec_sql()
    print('--- ok 3333---')
