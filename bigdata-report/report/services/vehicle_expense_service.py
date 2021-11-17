# -*- coding: utf-8 -*-
"""
车辆使用费异常检查
@author: WangShuo

"""

import time

from report.commons.connect_kudu import prod_execute_sql
from report.commons.db_helper import query_kudu_data
from report.commons.logging import get_logger
from report.commons.tools import not_empty

import jieba.analyse as analyse
import jieba
from string import punctuation
from string import digits
import re
from report.commons.settings import CONN_TYPE

log = get_logger(__name__)

"""
车辆使用费
01_datamart_layer_007_h_cw_df.finance_car_bill 
"""

def check_54_invoice():
    start_time = time.perf_counter()
    sql = """
         UPSERT into analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets
        SELECT bill_id, 
        '54' as unusual_id,
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
        '车辆使用费',
        0 as meeting_amount
        from 01_datamart_layer_007_h_cw_df.finance_car_bill 
    where billingdate is not null and substr(billingdate,1,4)<>cast(year(now()) as string)
                """

    prod_execute_sql(sqltype='insert', sql=sql)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* check_54_invoice SQL耗时 {consumed_time} sec')
    # dis_connection()


def check_56_consistent_amount():
    start_time = time.perf_counter()
    sql = """
             UPSERT into analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets
        SELECT bill_id, 
        '56' as unusual_id,
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
        '车辆使用费',
        0 as meeting_amount
    from 01_datamart_layer_007_h_cw_df.finance_car_bill  
    where check_amount > jzpz
        """
    prod_execute_sql(sqltype='insert', sql=sql)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* check_56_consistent_amount SQL耗时 {consumed_time} sec')
    # dis_connection()


def check_65_reimburse():
    start_time = time.perf_counter()
    sql = """
        UPSERT into analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets
        SELECT bill_id, 
        '65' as unusual_id,
        company_code,
        account_period,
        account_item,
        finance_number,
        cost_center,
        profit_center,
        '' as cart_head,
        bill_code,
        '' as origin_city,
        '' as destin_city,
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
        '车辆使用费',
        0 as meeting_amount
    from 01_datamart_layer_007_h_cw_df.finance_car_bill  
    where bill_id in (
        select a.bill_id from
            (select bill_id, (unix_timestamp(bill_apply_date, 'yyyyMMdd')-unix_timestamp(tb_times, 'yyyyMMdd'))/ (60 * 60 * 24) as diff_date 
            from  01_datamart_layer_007_h_cw_df.finance_car_bill where bill_apply_date > tb_times)a, (
            select standard_value from 01_datamart_layer_007_h_cw_df.finance_standard where unusual_id='65')b
            where a.diff_date > b.standard_value      
    )         
        """
    prod_execute_sql(sqltype='insert', sql=sql)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* check_65_reimburse SQL耗时 {consumed_time} sec')
    # dis_connection()


def check_64_credit():
    start_time = time.perf_counter()
    sql = """
    UPSERT into analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets
        SELECT bill_id, 
        '64' as unusual_id,
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
        '车辆使用费',
        0 as meeting_amount
        from 01_datamart_layer_007_h_cw_df.finance_car_bill where account_period is not null 
        and arrivedtimes is not null
        and cast(concat(substr(account_period,1,4),substr(account_period,6,2)) as int)> cast(replace(substr(arrivedtimes,1,7),'-','')  as int)  
        """
    prod_execute_sql(sqltype='insert', sql=sql)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* check_64_credit SQL耗时 {consumed_time} sec')
    # dis_connection()


def check_66_approve():
    start_time = time.perf_counter()
    sql = """
    UPSERT into analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets
with standard as (
  select standard_value from 01_datamart_layer_007_h_cw_df.finance_standard where unusual_id='66'
)
select bill_id, 
    '66' as unusual_id,
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
    '车辆使用费',
    0 as meeting_amount from 
01_datamart_layer_007_h_cw_df.finance_car_bill,standard
where 
arrivedtimes is not null
and 
tb_times is not null
and
((cast(replace(substr(arrivedtimes,1,7),'-','') as int)-cast(replace(substr(tb_times,1,7),'-','')  as int))-(select avg(cast(replace(substr(arrivedtimes,1,7),'-','')  as int)-cast(replace(substr(tb_times,1,7),'-','')  as int)) avgt from 
01_datamart_layer_007_h_cw_df.finance_car_bill))>standard.standard_value 
        """
    prod_execute_sql(sqltype='insert', sql=sql)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* check_66_approve SQL耗时 {consumed_time} sec')
    # dis_connection()


def main():
    # 需求 54 done
    # check_54_invoice()

    # 需求 56 done 未测试
    # check_56_consistent_amount()

    # 需求 64
    # check_64_credit()

    # 需求 65 done 未测试
    # check_65_reimburse()

    # 需求 66 done, checked
    check_66_approve()

    pass


def query_checkpoint_55_commoditynames():
    columns_ls = ['commodityname']
    columns_str = ",".join(columns_ls)

    sql = f'select distinct {columns_str} from 01_datamart_layer_007_h_cw_df.finance_car_bill where commodityname is not null and commodityname !="" '
    rd_df = query_kudu_data(sql, columns_ls)

    rd_df['category_class'] = rd_df.apply(lambda rd_df: cal_commodityname_function(rd_df['commodityname']), axis=1)
    # print(rd_df)

    category_class_ls = rd_df['category_class'].tolist()
    category_class_ls = list(filter(not_empty, category_class_ls))
    # 去重

    category_class_ls = list(set(category_class_ls))
    # print(len(category_class_ls), category_class_ls)
    return category_class_ls


def cal_commodityname_function(commodityname):
    """
    计算大类
    :param commodityname:
    :return:
    """
    category_class = None
    if commodityname:
        # print('**** 111 commodityname ==> ', commodityname, type(commodityname))

        if commodityname.find('*') > -1 and commodityname.find(',') > -1:
            commodityname_ls = commodityname.split(',')
            commodityname_ls = list(filter(not_empty, commodityname_ls))
            first_category = str(commodityname_ls[0]).strip()

            category_ls = first_category.split('*')
            category_ls = list(filter(not_empty, category_ls))
            category_class = str(category_ls[0]).strip()

        elif commodityname.find('*') > -1:
            category_ls = commodityname.split('*')
            category_ls = list(filter(not_empty, category_ls))
            category_class = str(category_ls[0]).strip()
        # else:
        #     category_class = commodityname
        #     print('**** 222 category_class ==> ' , category_class)

    return category_class


def get_car_bill_jiebaword():
    """
    抽取车辆使用费的关键字
    :return:
    """

    commodityname_ls = query_checkpoint_55_commoditynames()
    sql = "select distinct commodityname from 01_datamart_layer_007_h_cw_df.finance_car_bill where commodityname is not null and commodityname !=''"
    records = prod_execute_sql(conn_type='test', sqltype='select', sql=sql)
    jiebaword = []
    words = []
    for record in records:
        record_str = str(record[0])

        for commodityname in commodityname_ls:
            if record_str.find(commodityname) > -1:
                record_str = record_str.replace(commodityname, '')

        words.append(record_str)

    words1 = ' '.join(words)
    words2 = re.sub(r'[{}]+'.format(punctuation + digits), '', words1)
    words3 = re.sub("[a-z]", "", words2)
    words4 = re.sub("[A-Z]", "", words3)

    # print(words4)

    jieba.analyse.set_stop_words("/you_filed_algos/app/report/algorithm/stop_words.txt")
    jieba.analyse.set_idf_path("/you_filed_algos/app/report/algorithm/userdict.txt")
    final_list = analyse.extract_tags(words4, topK=80, withWeight=False, allowPOS=())

    return final_list

def pagination_car_records(categorys, good_keywords):
    """
    分页查询车辆使用费的记录
    :param categorys: 商品分类列表
    :param good_keywords: 商品关键字列表
    :return:
    """
    columns_ls = ['bill_id', 'commodityname', 'bill_type_name']
    columns_str = ",".join(columns_ls)
    sql = f"SELECT {columns_str} FROM 01_datamart_layer_007_h_cw_df.finance_official_bill "

    count_sql = 'SELECT count(a.bill_id) FROM ({sql}) a'.format(sql=sql)
    log.info(count_sql)
    records = prod_execute_sql(conn_type='test', sqltype='select', sql=count_sql)
    count_records = records[0][0]
    print('* count_records => ', count_records)

    ###### 拼装查询SQL
    where_sql = 'WHERE '
    condition_sql = ''

    if categorys:
        if good_keywords:
            categorys.extend(good_keywords)
    else:
        categorys = []
        if good_keywords:
            categorys.extend(good_keywords)

    if len(categorys) == 1:
        where_sql = where_sql + f'commodityname LIKE "%{categorys[0]}%"'
    elif len(categorys) > 1:
        for idx, category in enumerate(categorys):
            tmp_sql = f'commodityname LIKE "%{category}%"'

            if idx != len(categorys) - 1:
                condition_sql = condition_sql + tmp_sql + ' OR '
            else:
                condition_sql = condition_sql + tmp_sql

        where_sql = where_sql + condition_sql
    elif len(categorys) == 0:
        where_sql = where_sql + ' 1=1'

    order_sql = ' ORDER BY bill_id ASC '
    sql = sql + where_sql + order_sql

    return count_records, sql, columns_ls

if __name__ == "__main__":
    # main()

    # records = query_checkpoint_55_commoditynames()
    # print(records)

    final_list = get_car_bill_jiebaword()

    for word in final_list:
        print(word)



