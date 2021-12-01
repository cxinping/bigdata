# -*- coding: utf-8 -*-

import time

# from report.commons.connect_kudu import prod_execute_sql
from report.commons.connect_kudu2 import prod_execute_sql

from report.commons.db_helper import query_kudu_data
from report.commons.logging import get_logger
from report.commons.tools import list_of_groups
from report.services.common_services import query_finance_ids_finance_all_targets
from report.commons.settings import CONN_TYPE

log = get_logger(__name__)

import sys

sys.path.append('/you_filed_algos/app')

import pandas as pd

# 设置显示最大列数 与 显示宽度
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 500)


def check_49_data():
    columns_ls = ['finance_offical_id', 'bill_id', 'bill_code', 'check_amount']  # 日期字段 account_period
    columns_str = ",".join(columns_ls)

    sql = 'select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_official_bill where check_amount > 0 AND bill_code is not NULL AND bill_code !=""  '.format(
        columns_str=columns_str)
    print(sql)

    start_time = time.perf_counter()
    rd_df = query_kudu_data(sql, columns_ls, conn_type=CONN_TYPE)
    # print(rd_df.head())
    # print(rd_df.dtypes)
    # print('*' * 50)
    # print(rd_df.describe())

    temp = rd_df.describe()[['check_amount']]
    mean_val = temp.at['mean', 'check_amount']  # 平均值
    std_val = temp.at['std', 'check_amount']  # 方差
    result = rd_df[rd_df['check_amount'] > std_val]

    print(f'* "check_amount"列计算的方差为 => {std_val}')
    print(result.head(5))

    finance_id_ls = result['finance_offical_id'].tolist()
    targes_finance_ids_ls = query_finance_ids_finance_all_targets(unusual_id='49')
    finance_id_ls = [x for x in finance_id_ls if x not in targes_finance_ids_ls]
    print(f'* len(targes_finance_ids_ls)={len(targes_finance_ids_ls)}')

    if len(finance_id_ls) > 0:
        exec_sql(finance_id_ls)  #
    else:
        print(' finance_id_ls length is 0 ')

    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* 查询耗时 {consumed_time} sec')


def exec_sql(finance_id_ls):
    print('* exec_sql ==> ', len(finance_id_ls))

    if finance_id_ls and len(finance_id_ls) > 0:
        group_ls = list_of_groups(finance_id_ls, 1000)
        # print(len(group_ls), group_ls)

        condition_sql = ''
        in_codition = 'finance_offical_id IN {temp}'

        for idx, group in enumerate(group_ls):
            if len(group) == 1:
                temp = in_codition.format(temp=str('("' + group[0] + '")'))
            else:
                temp = in_codition.format(temp=str(tuple(group)))

            if idx == 0:
                condition_sql = temp
            else:
                condition_sql = condition_sql + ' OR ' + temp

        # print(condition_sql)

        sql = """
        UPSERT INTO analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets    
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
        WHERE {condition_sql}
            """.format(condition_sql=condition_sql)  # .replace('\n', '').replace('\r', '').strip()

        #print(sql)

        try:
            start_time = time.perf_counter()
            prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)
            consumed_time = round(time.perf_counter() - start_time)
            print(f'*** 执行SQL耗时 {consumed_time} sec')
        except Exception as e:
            print(e)
            raise RuntimeError(e)


check_49_data()
print('--- ok ---')
