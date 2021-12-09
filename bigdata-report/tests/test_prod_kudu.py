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


def demo3():
    start_time0 = time.perf_counter()

    # prod_sql1 = 'select finance_travel_id,sales_name, sales_addressphone, sales_bank from 01_datamart_layer_007_h_cw_df.finance_travel_bill where finance_travel_id="92b750e8-f1c4-4a25-9c42-15c9aa49542a" '
    prod_sql = """
         select destin_name,sales_name,sales_addressphone,sales_bank,finance_travel_id,origin_name,invo_code from 01_datamart_layer_007_h_cw_df.finance_travel_bill
     where finance_travel_id='d8b37cb8-1b42-4de9-8cab-d1ed0586d120'
        """

    prod_sql2 = """
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


def select_finance_all_targets():
    prod_sql2 = """
            select * from analytic_layer_zbyy_cwyy_014_cwzbbg.finance_all_targets limit 10
        """

    # print(prod_sql2)
    records = prod_execute_sql(conn_type='prod', sqltype='select', sql=prod_sql2)

    print(records)
    print(len(records))

    for record in records:
        print(record)


def upsert_finance_all_targets():
    sql = """
        UPSERT INTO analytic_layer_zbyy_cwyy_014_cwzbbg.finance_all_targets   
            SELECT
            finance_offical_id as finance_id,
            bill_id,
            '49' as unusual_id,
            company_code,
            account_period,
            finance_number,
            cost_center,
            profit_center,
            cart_head,
            bill_code,
            bill_beg_date,
            bill_end_date,
            '' as origin_city,
            '' as destin_city,
            '' as beg_date,
            '' as end_date,
            apply_emp_name,
            '' as emp_name,
            '' as emp_code,
            company_name,
            0 as jour_amount,
            0 as accomm_amount,
            0 as subsidy_amount,
            0 as other_amount,
            check_amount,
            jzpz,
            '办公费' as target_classify,
            0 as meeting_amount,
            exp_type_name,
            '' as next_bill_id,
            '' as last_bill_id,
            appr_org_sfname,
            sales_address,
            '' as meet_addr,
            '' as sponsor,
            jzpz_tax,
            billingdate,
            '' as remarks,
            0 as hotel_amount,
            0 as total_amount,
            apply_id,
            base_apply_date,
            '' as scenery_name_details,
            '' as meet_num,
            0 as diff_met_date,
            0 as diff_met_date_avg,
            tb_times,
            receipt_city,
            commodityname,
            '' as category_name,
            iscompany,
            '' as origin_province,
            '' as destin_province,
            operation_time,
            doc_date,
            operation_emp_name,
            invoice_type_name,
            taxt_amount,
            original_tax_amount,
            js_times,
            '' as offset_day,
            '' as meet_lvl_name,
            '' as meet_type_name,
            0 as buget_limit,
            0 as sum_person,
            invo_number,
            invo_code,
            '' as city,
            importdate
            from 01_datamart_layer_007_h_cw_df.finance_official_bill
            WHERE finance_offical_id IN ('00022f7c-6dce-40ac-b975-eeba56f13118', '00060c90-5b7d-42d8-8dc7-b6c0843893fd')
        """

    prod_execute_sql(conn_type='prod', sqltype='insert', sql=sql)


def demo4():
    sql = "select unusual_id from 01_datamart_layer_007_h_cw_df.finance_unusual where isalgorithm ='1' and unusual_shell is not null order by unusual_id "
    records = prod_execute_sql(conn_type='prod', sqltype='select', sql=sql)
    print(len(records))
    for record in records:
        print(record)


def demo5():
    sql = "delete from  analytic_layer_zbyy_cwyy_014_cwzbbg.finance_all_targets where unusual_id in ('13', '14')"
    prod_execute_sql(conn_type='prod', sqltype='insert', sql=sql)
    print('--- ok ---')

def demo6():
    sql1 = """
    """
    #prod_execute_sql(conn_type='prod', sqltype='insert', sql=sql1)

    sql2 = "select * from 01_datamart_layer_007_h_cw_df.finance_unusual where unusual_id = '13'"
    records = prod_execute_sql(conn_type='prod', sqltype='select', sql=sql2)
    for record in records:
        print(record)

if __name__ == "__main__":
    # select_finance_all_targets()
    # upsert_finance_all_targets()
    # demo4()

    demo5()
    #demo6()