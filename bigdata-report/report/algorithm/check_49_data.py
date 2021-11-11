# -*- coding: utf-8 -*-

import time

from report.commons.connect_kudu import prod_execute_sql
from report.commons.db_helper import query_kudu_data
from report.commons.logging import get_logger
from report.commons.tools import list_of_groups

log = get_logger(__name__)

import sys
sys.path.append('/you_filed_algos/app')

def check_49_data():
    columns_ls = ['finance_offical_id', 'bill_id', 'check_amount']
    columns_str = ",".join(columns_ls)

    sql = 'select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_official_bill where check_amount > 0  limit 10000'.format(
        columns_str=columns_str)
    start_time = time.perf_counter()
    rd_df = query_kudu_data(sql, columns_ls)
    # print(rd_df.head())
    # print(rd_df.dtypes)
    # print('*' * 50)
    # print(rd_df.describe())

    temp = rd_df.describe()[['check_amount']]
    mean_val = temp.at['mean', 'check_amount']  # 平均值
    std_val = temp.at['std', 'check_amount']  # 方差

    result = rd_df[rd_df['check_amount'] > std_val]
    #print(result)

    bill_id_ls = result['bill_id'].tolist()
    exec_sql(bill_id_ls)
    #print(bill_id_ls)

    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* 查询耗时 {consumed_time} sec')


def exec_sql(bill_id_ls):
    print('exec_sql ==> ',len(bill_id_ls))

    if bill_id_ls and len(bill_id_ls) > 0:
        group_ls = list_of_groups(bill_id_ls, 1000)
        #print(len(group_ls), group_ls)

        condition_sql = ''
        in_codition = 'bill_id IN {temp}'

        for idx, group in enumerate(group_ls):
            temp = in_codition.format(temp=str(tuple(group)))
            if idx == 0 :
                condition_sql = temp
            else:
                condition_sql = condition_sql + ' OR ' + temp

        #print(condition_sql)

    sql = """
    UPSERT INTO analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets
    SELECT bill_id ,
    '49' as unusual_id ,
     company_code ,
     account_period ,
     finance_number ,
     cost_center ,
     profit_center ,
     cart_head ,
     bill_code ,
     bill_beg_date ,
     bill_end_date ,
     ' ' as origin_city ,
     ' ' as destin_city ,
     ' ' as beg_date ,
     ' ' as end_date ,
     apply_emp_name ,
     ' ' as emp_name ,
     ' ' as emp_code ,
     company_name ,
     0 as jour_amount ,
     0 as accomm_amount ,
     0 as subsidy_amount ,
     0 as other_amount ,
     check_amount ,
     jzpz ,
    '办公费' as target_classify ,
     0 as meeting_amount ,
     exp_type_name ,
     ' ' as next_bill_id ,
     ' ' as last_bill_id ,
     appr_org_sfname ,
     sales_address ,
     ' ' as meet_addr ,
     ' ' as sponsor ,
     jzpz_tax ,
     billingdate ,
     ' ' as remarks ,
     0 as hotel_amount ,
     0 as total_amount ,
     apply_id ,
     base_apply_date ,
     ' ' as scenery_name_details ,
     ' ' as meet_num ,
     0 as diff_met_date ,
     0 as diff_met_date_avg ,
     tb_times ,
     receipt_city ,
     importdate ,
     commodityname ,
     ' ' as category_name,
    iscompany
    FROM 01_datamart_layer_007_h_cw_df.finance_official_bill
    WHERE {condition_sql}
        """.format(condition_sql=condition_sql).replace('\n', '').replace('\r', '').strip()
    print(sql)
    start_time = time.perf_counter()
    prod_execute_sql(conn_type='test', sqltype='insert', sql=sql)
    consumed_time = round(time.perf_counter() - start_time)
    print(f'*** 执行SQL耗时 {consumed_time} sec')


check_49_data()
print('--- ok ---')
