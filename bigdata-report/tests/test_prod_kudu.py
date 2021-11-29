# -*- coding: utf-8 -*-
from report.commons.connect_kudu2 import prod_execute_sql, dis_connection
import time


def demo1():
    sql = """
    describe analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets
    """

    # select * from 01_datamart_layer_007_h_cw_df.finance_unusual
    # describe analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets
    # select * from analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets where unusual_id='33'

    # records = prod_execute_sql(conn_type='prod', sqltype='select', sql=sql)
    # for record in records:
    #     print(record)

    print('===' * 30)

    sql1 = """
    msck repair table  02_logical_layer_007_h_lf_cw.finance_meeting_linshi_analysis
    """
    prod_execute_sql(conn_type='test', sqltype='insert', sql=sql1)

def demo2():
    start_time = time.perf_counter()
    # 连接 KUDU 库下的表
    # rd_df = getKUDUdata('select * from python_test_kudu.irisdataset limit 10')
    # 连接 HIVE 表
    # rd_df = getKUDUdata('select * from python_test.irisdataset limit 100')

    # 连接生产库
    # rd_df = getKUDUdata('select finance_travel_id from 01_datamart_layer_007_h_cw_df.finance_travel_bill limit 3')
    # print(rd_df)
    # print( f'* rd_df ==> {rd_df.shape[0]} rows * {rd_df.shape[1]} columns')
    # consumed_time = round(time.perf_counter() - start_time)
    # print(f'* consumed_time={consumed_time}')
    # dis_connection()

    # sql = 'select finance_travel_id from 01_datamart_layer_007_h_cw_df.finance_travel_bill t where t.check_amount > t.jzpz limit 5'
    # print(sql)
    # records = prod_execute_sql(sqltype='select', sql=sql)
    # print('111*** query_kudu_data=>', len(records))
    #
    # sql = 'select finance_travel_id from 01_datamart_layer_007_h_cw_df.finance_travel_bill t where t.check_amount > t.jzpz limit 5'
    # print(sql)
    # records = prod_execute_sql(sqltype='select', sql=sql)
    # print('222*** query_kudu_data=>', len(records))

    try:
        prod_sql = 'select finance_travel_id,bill_id from 01_datamart_layer_007_h_cw_df.finance_travel_bill t limit 5'
        test_sql = 'select * from 01_datamart_layer_007_h_cw_df.payment_result_info limit 5'
        print(test_sql)
        records = prod_execute_sql(conn_type='prod', sqltype='select', sql=prod_sql)
        print('111 *** query_kudu_data=>', len(records))
        for record in records:
            print(record)

        # dis_connection()
        # print('-- ok --')

        records = prod_execute_sql(conn_type='prod', sqltype='select', sql=prod_sql)
        print('222 *** query_kudu_data=>', len(records))

    except Exception as e:
        print(e)


if __name__ == "__main__":
    start_time0 = time.perf_counter()

    #prod_sql1 = 'select finance_travel_id,sales_name, sales_addressphone, sales_bank from 01_datamart_layer_007_h_cw_df.finance_travel_bill where finance_travel_id="92b750e8-f1c4-4a25-9c42-15c9aa49542a" '
    prod_sql = """
     select destin_name,sales_name,sales_addressphone,sales_bank,finance_travel_id,origin_name,invo_code from 01_datamart_layer_007_h_cw_df.finance_travel_bill
 where finance_travel_id='d8b37cb8-1b42-4de9-8cab-d1ed0586d120'
    """

    prod_sql2= """
    select count(finance_travel_id) from (select destin_name,sales_name,sales_addressphone,sales_bank,finance_travel_id,origin_name,invo_code,sales_taxno 
from 01_datamart_layer_007_h_cw_df.finance_travel_bill         
where !(sales_name is  null and  sales_addressphone is null and sales_bank is null and origin_name is  null and destin_name is  null and sales_taxno is null ) 
and left(account_period,4) ='2016') a
    """

    print(prod_sql2)
    records = prod_execute_sql(conn_type='test', sqltype='select', sql=prod_sql2)
    print(len(records))

    for record in records:
        print(record)

    consumed_time0 = (time.perf_counter() - start_time0)
    print(f'* 取数耗时 => {consumed_time0} sec, records={len(records)}')









