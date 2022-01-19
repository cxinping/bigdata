# -*- coding: utf-8 -*-

# from gevent import monkey;
# monkey.patch_all(thread=False)
# import gevent
# from gevent.pool import Pool

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
    # sql = 'update 01_datamart_layer_007_h_cw_df.finance_unusual set sign_status ="1" '
    # sql = 'update 01_datamart_layer_007_h_cw_df.finance_unusual set sign_status ="0" where unusual_id="46" '
    sql_lvl1 = 'update 01_datamart_layer_007_h_cw_df.finance_unusual set unusual_level ="1" WHERE unusual_id in ("0100", "01", "03" , "04" , "05", "16" , "20", "23" , "2400", "25", "27" , "37" , "40" , "4100", "5400", "41", "43" ,"46" , "53" , "54", "56" , "63" , "66"    ) '
    sql_lvl2 = 'update 01_datamart_layer_007_h_cw_df.finance_unusual set unusual_level ="2" WHERE unusual_id in ("02", "11" , "12" , "22", "28" , "31", "39" , "42", "52", "65"  ) '
    sql_lvl3 = 'update 01_datamart_layer_007_h_cw_df.finance_unusual set unusual_level ="3" WHERE unusual_id in ("0101", "26" , "32" , "35", "44" , "48", "55" , "57", "59"   ) '
    sql_lvl4 = 'update 01_datamart_layer_007_h_cw_df.finance_unusual set unusual_level ="4" WHERE unusual_id in ("2401", "54", "01", "41", "47", "50", "51", "58", "64", "2401", "24" , "08" , "09", "17" , "21", "29" , "30", "34" , "38" , "45"  ) '

    log.info(sql_lvl1)
    prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql_lvl1)
    prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql_lvl2)
    prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql_lvl3)
    prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql_lvl4)

    # for unusual_id in ['06', '07', '10', '13', '14', '15', '18', '19', '33', '36', '49', '60', '61', '62']:
    #     del_sql = f'delete from 01_datamart_layer_007_h_cw_df.finance_unusual where unusual_id = "{unusual_id}" '
    #     # prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=del_sql)

    sql2 = 'select unusual_point, unusual_id,sign_status,unusual_level from 01_datamart_layer_007_h_cw_df.finance_unusual order by unusual_id asc '
    # log.info(sql2)
    records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sql2)
    print('总记录数 =>', len(records))
    for record in records:
        unusual_point = str(record[0])
        unusual_id = str(record[1])
        sign_status = str(record[2])
        unusual_level = str(record[3])
        print(
            f'unusual_id={unusual_id},sign_status={sign_status},unusual_point={unusual_point},unusual_level={unusual_level}')


def demo3():
    #     sel_sql1 = "select * FROM 01_datamart_layer_007_h_cw_df.finance_data_process WHERE from_unixtime(unix_timestamp(to_date(importdate),'yyyy-MM-dd'),'yyyyMMdd') = '20220107' AND process_status = 'sucess'  ORDER BY step_number ASC  "
    #     sel_sql2 = "select * FROM 01_datamart_layer_007_h_cw_df.finance_data_process "
    #     sel_sql3 = """
    #     select cc.* from 01_datamart_layer_007_h_cw_df.finance_data_process cc,
    # (select distinct * from (
    # select
    # step_number,
    # first_value(daily_end_date) over(partition by step_number order by daily_end_date desc) max_end_date
    # FROM 01_datamart_layer_007_h_cw_df.finance_data_process
    # WHERE from_unixtime(unix_timestamp(to_date(importdate),'yyyy-MM-dd'),'yyyyMMdd') = '20220105'
    # AND process_status = 'sucess'
    # ORDER BY step_number ASC) zz) bb
    # where cc.step_number=bb.step_number and cc.daily_end_date=bb.max_end_date
    #     """
    sel_sql = 'SELECT unusual_shell,isalgorithm,unusual_id FROM 01_datamart_layer_007_h_cw_df.finance_unusual WHERE cost_project="会议费" AND sign_status="1" ORDER BY unusual_id ASC'
    sel_sql2 = 'SELECT id,company_name,company_code,company_old_code,iscompany FROM 01_datamart_layer_007_h_cw_df.finance_company_code '
    sel_sql3 = """
    select * FROM 01_datamart_layer_007_h_cw_df.finance_data_process
    WHERE ( from_unixtime(unix_timestamp(to_date(importdate),'yyyy-MM-dd'),'yyyyMMdd') = '20220117' or  importdate = '20220117' )
ORDER BY step_number ASC
    """

    query_date = '20220118'
    sel_sql4 = f"""
select * from (
select
   step_number,
   row_number() over (partition by target_classify,step_number order by daily_end_date desc) as numbers,
   process_id,       
   process_status,   
   target_classify,   
   daily_start_date,
   daily_end_date,   
   operate_desc,   
   orgin_source,   
   destin_source,   
   importdate  
from 
   01_datamart_layer_007_h_cw_df.finance_data_process
) y where y.numbers=1 
AND    importdate = '{query_date}'  
order by step_number
            """

    print(sel_sql4)

    records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sel_sql4)
    print('records => ', len(records))

    for record in records:
        # print('unusual_id=', record[2])
        print(record)
        # print()


def demo4():
    sql = 'select distinct commodityname from 01_datamart_layer_007_h_cw_df.finance_travel_bill where commodityname is not null and commodityname !='''

    year_month_day = '20220118'

    del_sql = f"""
    delete from 01_datamart_layer_007_h_cw_df.finance_data_process WHERE ( from_unixtime(unix_timestamp(to_date(importdate),'yyyy-MM-dd'),'yyyyMMdd') = '{year_month_day}' OR importdate = '{year_month_day}' )
    AND step_number in ('6', '7', '8', '9')
    """
   # prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=del_sql)

    sel_sql = f"""
    select * FROM 01_datamart_layer_007_h_cw_df.finance_data_process WHERE ( from_unixtime(unix_timestamp(to_date(importdate),'yyyy-MM-dd'),'yyyyMMdd') = '{year_month_day}' OR importdate = '{year_month_day}' ) AND 
    process_status = 'sucess'  ORDER BY step_number ASC
    """

    records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sel_sql)
    for record in records:
        print(record)
        # print()


def demo5():
    sel_sql = """
    select pstng_date, account_period  from 01_datamart_layer_007_h_cw_df.finance_travel_bill limit 5 
    """
    records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sel_sql)
    for record in records:
        print(record)
        print()


def process_finance_unusual2():
    sql = """
    
    """

    log.info(sql)
    prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)


def query_travel():
    year = '2021'
    test_limit_cond = ''
    columns_ls = ['destin_name', 'sales_name', 'sales_addressphone', 'sales_bank', 'finance_travel_id', 'origin_name',
                  'invo_code', 'sales_taxno']

    columns_str = ",".join(columns_ls)
    sql = """
    select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill 
        where !(sales_name is  null and  sales_addressphone is null and sales_bank is null and origin_name is  null and destin_name is  null and sales_taxno is null ) AND left(account_period,4)  >= '{year}'  
        {test_limit_cond}
    """.format(columns_str=columns_str, year=year,
               test_limit_cond=test_limit_cond).replace('\n', '').replace('\r', '').strip()
    count_sql = 'select count(a.finance_travel_id) from ({sql}) a'.format(sql=sql)
    log.info(count_sql)
    records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=count_sql)
    count_records = records[0][0]
    log.info(f'* count_records ==> {count_records}')


if __name__ == '__main__':
    # del_history_exception_data()
    # process_finance_shell_daily()
    # process_finance_unusual()

    # demo1()
    # demo2()
    # exec_sql()
    #demo3()
    #demo4()
    demo5()
    #process_finance_unusual2()
    #query_travel()

    # r = '2021012'
    # print(r[0:4])

    print('--- ok , executed 111 ---')
