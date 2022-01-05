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
    sql = "delete from analytic_layer_zbyy_cwyy_014_cwzbbg.finance_all_targets where unusual_id in ('09' ) "  # ('12' ,'13', '14', '34', '49' )
    print(sql)
    prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)


def process_finance_shell_daily():
    # sql1 = "delete from 01_datamart_layer_007_h_cw_df.finance_shell_daily "
    # prod_execute_sql(conn_type='prod', sqltype='insert', sql=sql1)

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
    # sql2 = "delete from 01_datamart_layer_007_h_cw_df.finance_shell_daily where unusual_id = '01' "
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

    # sql4 = """
    # delete from analytic_layer_zbyy_cwyy_014_cwzbbg.finance_all_targets
    # where  unusual_id in ('49')
    # """

    # print(sql4)
    # prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql4)

    # sql4 = """
    # select account_period,finance_travel_id,bill_id,plane_beg_date,plane_end_date,plane_origin_name,plane_destin_name,plane_check_amount from 01_datamart_layer_007_h_cw_df.finance_travel_bill WHERE plane_check_amount > 0 AND isPlane = 'plane' AND ( plane_origin_name is not null AND plane_destin_name is not null)
    # AND (plane_beg_date is not null AND plane_beg_date !='') order by account_period desc limit 10
    # """

    #     sql4 = """
    #     select account_period
    #     from 01_datamart_layer_007_h_cw_df.finance_car_bill where account_period >= '2021010' limit 10
    #     """
    #
    #     sql4 = """
    #     describe 01_datamart_layer_007_h_cw_df.temp_performance_bill
    #     """
    #
    #     sql4 = """
    #     select * from 01_datamart_layer_007_h_cw_df.temp_performance_bill order by order_number asc
    #     """
    #
    #     sql4 = """
    #     select length(invo_code),count(*) from  01_datamart_layer_007_h_cw_df.finance_travel_bill
    # group by length(invo_code)
    #     """

    # sql4 = """
    #     select * from 01_datamart_layer_007_h_cw_df.finance_travel_bill where sales_taxno is NULL limit 5
    #     """

    # sql4 = f'SELECT * FROM 01_datamart_layer_007_h_cw_df.finance_data_process'

    sql4 = 'select * from 01_datamart_layer_007_h_cw_df.finance_data_process'

    # sql4 = 'select count(*) from 02_logical_layer_007_h_lf_cw.finance_travel_linshi_analysis'
    log.info(sql4)
    records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sql4)
    for record in records:
        print(record)
        print()


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
    # print(unusual_ls)

    # unusual_ls = ['01', '02', '03', '04', '08', '09', '10', '15', '16', '17', '20', '21', '23']

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


def demo3():
    sql = """
    insert into analytic_layer_zbyy_cwyy_014_cwzbbg.finance_all_targets
(finance_id,bill_id,unusual_id,cart_head,company_code,account_period,finance_number,profit_center,bill_code,bill_beg_date,bill_end_date,origin_city,destin_city,beg_date,end_date,apply_emp_name,company_name,emp_name,emp_code,jour_amount,accomm_amount,subsidy_amount,other_amount,check_amount,jzpz,target_classify,sales_address,jzpz_tax,receipt_city,importdate,remarks,hotel_amount,total_amount,approve_name,iscompany,invo_code,invo_number,invoice_type_name,billingdate,commodityname,amounttax,taxtp_name,focus_amount)
select uuid(),zz.* from (
select distinct                           
a.bill_id, 
'09' ,
a.cart_head,
a.company_code,
a.account_period,
a.finance_number,
a.profit_center,
a.bill_code,
a.bill_beg_date,
a.bill_end_date,
a.origin_name,
a.destin_name,
a.jour_beg_date,
a.jour_end_date,
a.apply_emp_name,
a.member_company_name,
a.member_emp_name,
a.member_emp_id,
a.jour_amount,
a.accomm_amount,
a.subsidy_amount,
a.other_amount,
a.check_amount,
a.jzpz,
'差旅费',
a.sales_address,
a.jzpz_tax,
a.receipt_city,
a.importdate,
b.ext_reason,
b.hotel_amount,
b.total_amount,
a.approve_name,
a.iscompany,
a.invo_code,
a.invo_number,
a.invoice_type_name,
a.billingdate,
a.commodityname,
a.amounttax,a.taxtp_name,
a.jzpz as focus_amount
from 01_datamart_layer_007_h_cw_df.finance_travel_bill a,
 (
  select bill_id,hotel_amount,total_amount,ext_reason
  from 03_basal_layer_zfybxers00.zfybxers00_z_rma_travel_accomm_m 
  where iuuc_flag !='D' and exp_type_name='差旅费' 
  and hotel_amount > total_amount 
  and is_ext_stand='1'
  ) b 
  where a.bill_code != '' and 
        (b.ext_reason is  null or b.ext_reason = '') and 
        a.bill_code is not null and
    a.account_period is not null and 
    a.receipt_city is not null and 
    a.receipt_city != '#N/A' and
    a.receipt_city != '市' and 
    a.receipt_city != '' and
    a.profit_center != '' and
    a.bill_beg_date != '' and
    a.bill_end_date != '' and
    a.bill_id=b.bill_id )zz
    """
    print(sql)
    prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)

    sel_sql = """
    select         uuid(),        '07',        year_r,        receipt_city,        traf_name,        times,        if(cast(year_r as int)-cast(last_year as int)=1,last_times,0) as last_times,        isCompany from (select        year_r,       receipt_city,       traf_name,       times,       lag(times,1,0) over(partition by receipt_city,traf_name,iscompany order by year_r) as last_times,       lag(year_r,1,'0') over(partition by receipt_city,traf_name,iscompany order by year_r) as last_year,       iscompany from (select        year_r,       receipt_city,       traf_name,       count(finance_travel_id) as times,       iscompanyfrom     (        select         z.finance_travel_id,        z.member_emp_id,        substr(z.account_period,1,4)as year_r,        z.iscompany,        z.receipt_city,        z.bill_id,        z.emp_id,        (case         when z.traf_name like '%飞机%' then '飞机'        when z.traf_name like '%火车%' then '火车'        when z.traf_name like '%普通客车%' then '普通客车'        when z.traf_name like '%动车%' then '高铁(动车)'        when z.traf_name like '%高铁%' then '高铁(动车)'        else '其他' end) as traf_name            from         (            select distinct                    a.finance_travel_id,                    a.bill_id,                    a.member_emp_id,                    a.account_period,                    a.iscompany,                    a.receipt_city,                    b.emp_id,                    b.traf_name            from    01_datamart_layer_007_h_cw_df.finance_travel_bill a,                    01_datamart_layer_007_h_cw_df.finance_rma_travel_journey b            where   a.bill_id=b.bill_id and a.member_emp_id=b.emp_id and                      b.exp_type_name='差旅费' and b.iuuc_flag!='D' and                     a.receipt_city is not null and                     a.iscompany ='jituan'                         ) z     )zz group by year_r,         receipt_city,         traf_name,         iscompanyorder by receipt_city,         traf_name,         iscompany)w)z
    """
    #records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sql)
    #print(len(records))


if __name__ == '__main__':
    #del_history_exception_data()
    # process_finance_shell_daily()
    # process_finance_unusual()
    # demo1()
    # demo2()
    #exec_sql()

    demo3()
    print('--- ok , executed 333 ---')
