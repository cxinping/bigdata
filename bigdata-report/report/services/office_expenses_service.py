# -*- coding: utf-8 -*-
"""
办公费异常检查
@author: WangShuo

"""

import pandas as pd
import time

from report.commons.connect_kudu import prod_execute_sql
from report.commons.db_helper import query_kudu_data
from report.commons.logging import get_logger
from report.commons.tools import not_empty
from report.services.vehicle_expense_service import cal_commodityname_function

import jieba.analyse as analyse
import jieba
from string import punctuation
from string import digits
import re

log = get_logger(__name__)


def check_41_credit():
    start_time = time.perf_counter()
    sql = """
     UPSERT into analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets
    SELECT bill_id, 
    '41' as unusual_id,
    company_code,
    account_period,
    account_item,
    finance_number,
    cost_center,
    profit_center,
    '' as cart_head,
    bill_code,
    ''   as  origin_city,
    ''  as destin_city,
    base_beg_date  as beg_date,
    base_end_date  as end_date,
    '' as emp_name,
    '' as emp_code,
    0 as jour_amount,
    0 as accomm_amount,
    0 as subsidy_amount,
    0 as other_amount,
    check_amount,
    jzpz,
    '办公费',
    0 as meeting_amount
    from 01_datamart_layer_007_h_cw_df.finance_official_bill 
where billingdate is not null and substr(billingdate,1,4)<>cast(year(now()) as string)
            """

    prod_execute_sql(sqltype='insert', sql=sql)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* check_41_credit SQL耗时 {consumed_time} sec')
    # dis_connection()


def check_43_consistent_amount():
    start_time = time.perf_counter()
    sql = """
    UPSERT into analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets
    SELECT bill_id, 
    '43' as unusual_id,
    company_code,
    account_period,
    account_item,
    finance_number,
    cost_center,
    profit_center,
    '' as cart_head,
    bill_code,
    ''   as  origin_city,
    ''  as destin_city,
    base_beg_date  as beg_date,
    base_end_date  as end_date,
    '' as emp_name,
    '' as emp_code,
    0 as jour_amount,
    0 as accomm_amount,
    0 as subsidy_amount,
    0 as other_amount,
    check_amount,
    jzpz,
    '办公费',
    0 as meeting_amount
    from 01_datamart_layer_007_h_cw_df.finance_official_bill  
    where check_amount > jzpz
        """
    prod_execute_sql(sqltype='insert', sql=sql)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* check_43_consistent_amount SQL耗时 {consumed_time} sec')
    # dis_connection()


def query_kudu_data(sql, columns):
    """
    发票日期异常检查
    :return:
    """
    records = prod_execute_sql(conn_type='test', sqltype='select', sql=sql)
    log.info('***' * 10)
    log.info('*** query_kudu_data=>' + str(len(records)))
    log.info('***' * 10)

    dataFromKUDU = []
    for item in records:
        record = []
        if columns:
            for idx in range(len(columns)):
                # print(item[idx], type(item[idx]))

                if str(item[idx]) == "None":
                    record.append(None)
                elif str(type(item[idx])) == "<java class 'JDouble'>":
                    record.append(float(item[idx]))
                else:
                    record.append(str(item[idx]))

        dataFromKUDU.append(record)

    df = pd.DataFrame(data=dataFromKUDU, columns=columns)
    return df


def check_49_data():
    columns_ls = ['finance_travel_id', 'bill_id', 'check_amount']
    columns_str = ",".join(columns_ls)

    sql = 'select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_official_bill limit 5'.format(
        columns_str=columns_str)
    # count_sql = 'select count(a.finance_travel_id) from ({sql}) a'.format(sql=sql)
    # log.info(count_sql)
    # records = prod_execute_sql(conn_type='test', sqltype='select', sql=count_sql)
    # count_records = records[0][0]
    # log.info(f'* count_records ==> {count_records}')

    start_time = time.perf_counter()
    # records = prod_execute_sql(conn_type='test', sqltype='select', sql=sql)
    rd_df = query_kudu_data(sql, columns_ls)
    print(rd_df.head())
    print(rd_df.dtypes)

    print('*' * 10)
    # rd_df['jzpz_2'] = rd_df[['jzpz']].sum(axis=1)
    # temp = rd_df[["check_amount" ]]
    # rd_df["avg"] = temp.mean(axis=1)
    print(rd_df.describe())

    temp = rd_df.describe()[['check_amount']]
    mean_val = temp.at['mean', 'check_amount']  # 平均值
    std_val = temp.at['std', 'check_amount']  # 方差

    result = rd_df[rd_df['check_amount'] > std_val]
    print(result)

    print(result['finance_travel_id'].tolist())

    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* 查询耗时 {consumed_time} sec')

    print('--- ok ---')


def check_51_credit():
    start_time = time.perf_counter()
    sql = """
     UPSERT into analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets
select bill_id, 
    '51' as unusual_id,
    company_code,
    account_period,
    account_item,
    finance_number,
    cost_center,
    profit_center,
    '' as cart_head,
    bill_code,
    ''   as  origin_city,
    ''  as destin_city,
    base_beg_date  as beg_date,
    base_end_date  as end_date,
    '' as emp_name,
    '' as emp_code,
    0 as jour_amount,
    0 as accomm_amount,
    0 as subsidy_amount,
    0 as other_amount,
    check_amount,
    jzpz,
    '办公费',
    0 as meeting_amount  from 01_datamart_layer_007_h_cw_df.finance_official_bill where account_period is not null 
and arrivedtimes is not null
and cast(concat(substr(account_period,1,4),substr(account_period,6,2)) as int)> cast(replace(substr(arrivedtimes,1,7),'-','')  as int)
         """
    prod_execute_sql(sqltype='insert', sql=sql)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* check_51_credit SQL耗时 {consumed_time} sec')
    # dis_connection()


def check_52_reimburse():
    start_time = time.perf_counter()
    sql = """
     with standard as (
  select standard_value from 01_datamart_layer_007_h_cw_df.finance_standard where unusual_id='52'
)
select a.bill_id, 
    '52' as unusual_id,
    a.company_code,
    a.account_period,
    a.account_item,
    a.finance_number,
    a.cost_center,
    a.profit_center,
    '' as cart_head,
    a.bill_code,
    ''   as  origin_city,
    ''  as destin_city,
    a.base_beg_date  as beg_date,
    a.base_end_date  as end_date,
    '' as emp_name,
    '' as emp_code,
    0 as jour_amount,
    0 as accomm_amount,
    0 as subsidy_amount,
    0 as other_amount,
    a.check_amount,
    a.jzpz,
    '办公费',
    0 as meeting_amount from 
01_datamart_layer_007_h_cw_df.finance_official_bill a 
left join 
01_datamart_layer_007_h_cw_df.finance_meeting_bill b 
on a.finance_travel_id=b.finance_travel_id
where a.bill_apply_date is not null
and b.met_endate is not null
and datediff(from_unixtime(unix_timestamp(a.bill_apply_date,'yyyyMMdd'),'yyyy-MM-dd'), from_unixtime(unix_timestamp(b.met_endate,'yyyyMMdd'),'yyyy-MM-dd'))>cast((select
standard_value from standard) as int)
         """
    prod_execute_sql(sqltype='insert', sql=sql)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* check_51_credit SQL耗时 {consumed_time} sec')
    # dis_connection()


def check_53_approve():
    start_time = time.perf_counter()
    sql = """
UPSERT into analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets
with standard as (
  select standard_value from 01_datamart_layer_007_h_cw_df.finance_standard where unusual_id='53'
)
select bill_id, 
    '53' as unusual_id,
    company_code,
    account_period,
    account_item,
    finance_number,
    cost_center,
    profit_center,
    '' as cart_head,
    bill_code,
    ''   as  origin_city,
    ''  as destin_city,
    base_beg_date  as beg_date,
    base_end_date  as end_date,
    '' as emp_name,
    '' as emp_code,
    0 as jour_amount,
    0 as accomm_amount,
    0 as subsidy_amount,
    0 as other_amount,
    check_amount,
    jzpz,
    '办公费',
    0 as meeting_amount from 
01_datamart_layer_007_h_cw_df.finance_official_bill,standard
where 
arrivedtimes is not null
and 
tb_times is not null
and
((cast(replace(substr(arrivedtimes,1,7),'-','') as int)-cast(replace(substr(tb_times,1,7),'-','')  as int))-(select avg(cast(replace(substr(arrivedtimes,1,7),'-','')  as int)-cast(replace(substr(tb_times,1,7),'-','')  as int)) avgt from 
01_datamart_layer_007_h_cw_df.finance_official_bill))>standard.standard_value

         """
    prod_execute_sql(sqltype='insert', sql=sql)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* check_53_approve SQL耗时 {consumed_time} sec')
    # dis_connection()


def main():
    # 需求41 done
    # check_41_credit()

    # 需求43 done
    # check_43_consistent_amount()

    # 需求 51 done, checked
    # check_51_credit()

    # 需求52 done, checked
    # check_52_reimburse()

    # 需求 53, done, checked
    # check_53_approve()

    # 算法49
    check_49_data()

    pass


def query_checkpoint_42_commoditynames():
    columns_ls = ['commodityname']
    columns_str = ",".join(columns_ls)

    sql = f'select distinct {columns_str} from 01_datamart_layer_007_h_cw_df.finance_official_bill where commodityname is not null '
    rd_df = query_kudu_data(sql, columns_ls)
    # print(len(rd_df))

    rd_df['category_class'] = rd_df.apply(lambda rd_df: cal_commodityname_function(rd_df['commodityname']), axis=1)
    # print(rd_df)

    category_class_ls = rd_df['category_class'].tolist()
    category_class_ls = list(filter(not_empty, category_class_ls))

    # 去重
    category_class_ls = list(set(category_class_ls))

    # print(len(category_class_ls))
    # print(category_class_ls)

    return category_class_ls





def get_office_bill_jiebaword():
    """
    jieba分词 精确模式
    :return:
    """

    commodityname_ls = query_checkpoint_42_commoditynames()
    sql = "select distinct commodityname from 01_datamart_layer_007_h_cw_df.finance_official_bill where commodityname is not null and commodityname !=''"
    records = prod_execute_sql(conn_type='test', sqltype='select', sql=sql)
    jiebaword = []
    words = []
    for record in records:
        record_str = str(record[0])

        for commodityname in commodityname_ls:
            if record_str.find(commodityname) > -1:
                record_str = record_str.replace('commodityname', '')

        words.append(record_str)

    words1 = ' '.join(words)
    words2 = re.sub(r'[{}]+'.format(punctuation + digits), '', words1)
    words3 = re.sub("[a-z]", "", words2)
    words4 = re.sub("[A-Z]", "", words3)

    # print(words4)

    jieba.analyse.set_stop_words("/you_filed_algos/app/report/algorithm/stop_words.txt")
    jieba.analyse.set_idf_path("/you_filed_algos/app/report/algorithm/userdict.txt")
    final_list = analyse.extract_tags(words4, topK=50, withWeight=False, allowPOS=())

    return final_list


if __name__ == "__main__":
    # main()
    # records = query_checkpoint_42_commoditynames()
    # print(records)

    final_list = get_office_bill_jiebaword()

    for word in final_list:
        print(word)
