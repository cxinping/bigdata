# -*- coding: utf-8 -*-
from gevent import monkey;

monkey.patch_all(thread=False)

import gevent
from gevent.pool import Pool

from report.commons.connect_kudu2 import prod_execute_sql
from report.commons.settings import CONN_TYPE
from report.commons.logging import get_logger
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED

log = get_logger(__name__)

"""

处理KUDUde历史数据

"""


def del_history_exception_data():
    sql = "delete from analytic_layer_zbyy_cwyy_014_cwzbbg.finance_all_targets where unusual_id in ('41' ,'45','50','51','52') "  # ('12' ,'13', '14', '34', '49' )
    print(sql)
    prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)


def process_finance_shell_daily():
    #sql1 = "delete from 01_datamart_layer_007_h_cw_df.finance_shell_daily "
    #prod_execute_sql(conn_type='prod', sqltype='insert', sql=sql1)

    sql2 = 'select * from 01_datamart_layer_007_h_cw_df.finance_shell_daily where daily_type="数据处理" '
    log.info(sql2)

    records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sql2)
    for record in records:
        print(record)


def demo1():
    sql1 = "update 01_datamart_layer_007_h_cw_df.finance_unusual set isalgorithm='2' where unusual_id = '01'"
    # prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql1)

    sql2 = f"""
    UPDATE 01_datamart_layer_007_h_cw_df.finance_shell_daily SET task_status="cancel", unusual_infor="系统重启，取消正在执行的执行检查点任务" WHERE task_status="doing"
    """
    #sql2 = "delete from 01_datamart_layer_007_h_cw_df.finance_shell_daily where unusual_id = '01' "
    # print(sql2)
    # prod_execute_sql(conn_type='prod', sqltype='insert', sql=sql2)
    # sql3 = "select * from  01_datamart_layer_007_h_cw_df.finance_unusual where unusual_id = '01' "
    # sql3 = "select count(1) from analytic_layer_zbyy_cwyy_014_cwzbbg.finance_all_targets where unusual_id = '49' "

    # sql4 = 'select * from 01_datamart_layer_007_h_cw_df.finance_shell_daily where unusual_point="51" '
    # sql5 = 'select * from 01_datamart_layer_007_h_cw_df.finance_unusual where unusual_id="15" '
    sql5 = 'select count(1) from analytic_layer_zbyy_cwyy_014_cwzbbg.finance_all_targets where unusual_id ="49" '
    # sql4 = 'select account_period from 01_datamart_layer_007_h_cw_df.finance_travel_bill WHERE plane_check_amount > 0 limit 10'
    log.info(sql5)
    records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sql5)
    for record in records:
        print(record)


def demo2():
    # sql = """
    #   delete from 01_datamart_layer_007_h_cw_df.finance_shell_daily where unusual_point="01"
    # """
    # sql2 = 'delete from 01_datamart_layer_007_h_cw_df.finance_shell_daily'
    # sql3 = "delete from analytic_layer_zbyy_cwyy_014_cwzbbg.finance_all_targets where unusual_id='04' "

    sql4 = """
    delete from analytic_layer_zbyy_cwyy_014_cwzbbg.finance_all_targets
    where  unusual_id in ('49')
    """

    # print(sql4)
    # prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql4)

    # sql4 = """
    # select account_period,finance_travel_id,bill_id,plane_beg_date,plane_end_date,plane_origin_name,plane_destin_name,plane_check_amount from 01_datamart_layer_007_h_cw_df.finance_travel_bill WHERE plane_check_amount > 0 AND isPlane = 'plane' AND ( plane_origin_name is not null AND plane_destin_name is not null)
    # AND (plane_beg_date is not null AND plane_beg_date !='') order by account_period desc limit 10
    # """

    sql4 = """
    select account_period
    from 01_datamart_layer_007_h_cw_df.finance_car_bill where account_period >= '2021010' limit 10
    """

    sql4 = """
    describe 01_datamart_layer_007_h_cw_df.temp_performance_bill
    """

    sql4 = """
    select * from 01_datamart_layer_007_h_cw_df.temp_performance_bill order by order_number asc
    """

    sql4 = """
    select length(invo_code),count(*) from  01_datamart_layer_007_h_cw_df.finance_travel_bill
group by length(invo_code)
    """

    log.info(sql4)
    records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sql4)
    for record in records:
        print(record)
        #print()


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

    #unusual_ls = ['01', '02', '03', '04', '08', '09', '10', '15', '16', '17', '20', '21', '23']

    sql = "select unusual_id, unusual_shell from  01_datamart_layer_007_h_cw_df.finance_unusual where isalgorithm='1' order by unusual_id"
    records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sql)

    # 方法1
    # for record in records:
    #     unusual_id = record[0]
    #     unusual_shell = record[1]
    #
    #     if unusual_id in unusual_ls:
    #         # log.info(unusual_shell)
    #
    #         try:
    #             if unusual_shell is not None and unusual_shell != 'None':
    #                 prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=unusual_shell)
    #                 log.info(f'成功执行检查点 {unusual_id} 的SQL')
    #         except Exception as e:
    #             print(e)
    #             log.info(f'error 执行检查点{unusual_id} 的SQL失败')

    # 方法2
    # pool = Pool(30)
    # results = []
    # for record in records:
    #     unusual_id = record[0]
    #     unusual_shell = record[1]
    #
    #     if unusual_id in unusual_ls:
    #         rst = pool.spawn(exec_task, unusual_id, unusual_shell)
    #         results.append(rst)
    # gevent.joinall(results)

    # 方法3
    threadPool = ThreadPoolExecutor(max_workers=30, thread_name_prefix="thr")
    all_task = []
    for record in records:
        unusual_id = record[0]
        unusual_shell = record[1]

        if unusual_id in unusual_ls:
            task = threadPool.submit(exec_task, unusual_id, unusual_shell)
            all_task.append(task)

    wait(all_task, return_when=ALL_COMPLETED)


def exec_task(unusual_id, unusual_shell):
    try:
        prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=unusual_shell)
        log.info(f'成功执行检查点 {unusual_id} 的SQL')
    except Exception as e:
        print(e)
        log.info(f'error 执行检查点{unusual_id} 的SQL失败')


def process_finance_unusual():
    sql = 'update 01_datamart_layer_007_h_cw_df.finance_unusual set sign_status ="1" '
    sql = 'update 01_datamart_layer_007_h_cw_df.finance_unusual set sign_status ="0" where unusual_id="46" '
    log.info(sql)
    prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)

    # sql2 = 'select * from 01_datamart_layer_007_h_cw_df.finance_unusual where unusual_id="15" '
    # log.info(sql2)
    # records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sql2)
    # for record in records:
    #     print(record)


if __name__ == '__main__':
    #del_history_exception_data()
    #process_finance_shell_daily()
    #process_finance_unusual()
    #demo1()
    demo2()
    #exec_sql()

    print('--- ok , executed 22 ---')


