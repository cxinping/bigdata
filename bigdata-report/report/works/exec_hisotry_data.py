# -*- coding: utf-8 -*-
from report.commons.connect_kudu2 import prod_execute_sql, dis_connection
from report.commons.settings import CONN_TYPE
from report.commons.logging import get_logger

log = get_logger(__name__)

"""

处理KUDUde历史数据

"""


def del_history_exception_data():
    sql = "delete from analytic_layer_zbyy_cwyy_014_cwzbbg.finance_all_targets where unusual_id in ('12', '13' ) " # ('12' ,'13', '14', '34', '49' )
    print(sql)
    prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)


def demo1():
    sql1 = "update 01_datamart_layer_007_h_cw_df.finance_unusual set isalgorithm='2' where unusual_id = '01'"
    # prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql1)

    sql2 = f"""
    UPDATE 01_datamart_layer_007_h_cw_df.finance_shell_daily SET task_status="cancel", unusual_infor="系统重启，取消正在执行的执行检查点任务" WHERE task_status="doing"
    """
    sql2 = "delete from 01_datamart_layer_007_h_cw_df.finance_shell_daily where unusual_id = '01' "
    # print(sql2)
    # prod_execute_sql(conn_type='prod', sqltype='insert', sql=sql2)

    #sql3 = "select * from  01_datamart_layer_007_h_cw_df.finance_unusual where unusual_id = '01' "
    sql3 = "select count(1) from analytic_layer_zbyy_cwyy_014_cwzbbg.finance_all_targets where unusual_id = '12' "
    # sql3 = 'select * from 01_datamart_layer_007_h_cw_df.finance_shell_daily where unusual_point="51" '
    print(sql3)
    records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sql3)
    for record in records:
        print(record)


def demo2():
    sql = """
      delete from 01_datamart_layer_007_h_cw_df.finance_shell_daily where unusual_point="01"
    """
    sql2 = 'delete from 01_datamart_layer_007_h_cw_df.finance_shell_daily'
    sql3 = "delete from analytic_layer_zbyy_cwyy_014_cwzbbg.finance_all_targets where unusual_id='01' "

    prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql3)


def exec_sql():
    # 差旅费
    travel_ls = ['01', '02', '03', '04', '08', '09', '10', '15', '16', '17', '20', '21', '22', '23']
    # 会议费
    meeting_ls = ['24', '25', '26', '27', '28', '29', '30', '32', '33', '34', '35', '37', '38', '39', '40']
    # 办公费
    office_ls = ['41', '42', '43', '45', '46', '47', '50', '51', '52', '53']
    # 车辆使用费
    car_ls = ['54', '55', '56', '58', '60', '62', '63', '64', '65', '66']

    unusual_ls = []
    unusual_ls.extend(travel_ls)
    unusual_ls.extend(meeting_ls)
    unusual_ls.extend(office_ls)
    unusual_ls.extend(car_ls)
    #print(unusual_ls)

    sql = "select unusual_id, unusual_shell from  01_datamart_layer_007_h_cw_df.finance_unusual where isalgorithm='1' order by unusual_id"
    records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sql)
    for record in records:
        unusual_id = record[0]
        unusual_shell = record[1]

        """
        检查点 01 不用执行
        检查点 02 执行时间长
        """

        if unusual_id  in ['01', '02'] and unusual_id in unusual_ls:
            log.info(unusual_shell)

            if '37' == '37':
                try:
                    if unusual_shell is not None and unusual_shell != 'None':
                        prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=unusual_shell)
                        log.info(f'成功执行 检查点{unusual_id} 的SQL')
                except Exception as e:
                    print(e)
                    log.info(f'error 执行检查点{unusual_id} 的SQL失败')


if __name__ == '__main__':
    #del_history_exception_data()
    demo1()
    #demo2()
    #exec_sql()
    print('--- ok 1 ---')
