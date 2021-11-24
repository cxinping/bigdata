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
    prod_sql = 'select finance_travel_id,sales_name, sales_addressphone, sales_bank from 01_datamart_layer_007_h_cw_df.finance_travel_bill where finance_travel_id="92b750e8-f1c4-4a25-9c42-15c9aa49542a" '
    print(prod_sql)
    records = prod_execute_sql(conn_type='test', sqltype='select', sql=prod_sql)
    print(records)
