# -*- coding: utf-8 -*-
"""
差旅费异常检查

@author: WangShuo
"""

import csv
import json
import os
import time

#from report.commons.connect_kudu import prod_execute_sql, dis_connection
from report.commons.connect_kudu2 import prod_execute_sql,dis_connection

from report.commons.logging import get_logger
from report.commons.tools import match_address
from report.commons.settings import CONN_TYPE
from string import punctuation
from string import digits
import re
import jieba.analyse as analyse
import jieba

log = get_logger(__name__)


"""
差旅费
01_datamart_layer_007_h_cw_df.finance_travel_bill   

"""

def demo():
    # del_sql = 'delete from 01_datamart_layer_007_h_cw_df.finance_all_targets  where unusual_id="10" '
    # prod_execute_sql(sqltype='insert', sql=del_sql)

    # sel_sql = "select count(bill_id) from 01_datamart_layer_007_h_cw_df.finance_all_targets where unusual_id='10' "
    # records = prod_execute_sql(sqltype='select', sql=sel_sql)
    # count_records = records[0][0]
    # log.info(f'查询记录总数 {count_records}')

    sel_sql = """
 select bill_id,invo_code,billingdate,travel_beg_date,travel_end_date,company_code,account_period,account_item,finance_number,cost_center,bill_code,origin_name,destin_name,jour_amount,accomm_amount,subsidy_amount,other_amount,check_amount,jzpz 
 from  01_datamart_layer_007_h_cw_df.finance_travel_bill   
 group by bill_id,invo_code,billingdate,travel_beg_date,travel_end_date,company_code,account_period,account_item,finance_number,cost_center,bill_code,origin_name,destin_name,jour_amount,accomm_amount,subsidy_amount,other_amount,check_amount,jzpz 
    """
    sel_sql = 'select a.* from ({sel_sql})a limit 1000000'.format(sel_sql=sel_sql)
    start_time = time.perf_counter()
    records = prod_execute_sql(sqltype='select', sql=sel_sql)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* 查询耗时 {consumed_time} sec')
    if records:
        log.info(f'查询记录总数 {len(records)}')

    dis_connection()
    print('--- ok --- ')


def main():
    # demo()

    # 需求1 done
    #check_01_invoice_data()

    # 需求2 未做
    # check_02_trip_data()

    # 需求3 done
    # check_03_consistent_amount()

    # 需求4 done
    # check_04_overlap_amount()

    # 需求6 暂时不做
    # check_06_reasonsubsidy_amount()

    # 需求7 done
    # check_07_continuous_business_trip()

    # 需求8 正在开发......
    pre_check_08_transportation()
    # check_08_transportation_expenses()

    # 需求10 done
    # check_10_beforeapply_amount()

    # 需求15 done
    # check_15_coststructure_data()

    # 需求19 正在开发......
    # check_19_accommodation_expenses()

    # 需求 13 算法 正在开发......
    # check_13_accommodation_price()

    # 需求21 done , checked
    #check_21_credit()

    # 需求22 done , checked
    #check_22_apply()

    # 需求23 done , checked
    #check_23_apply()

    pass


def check_01_invoice_data():
    sql = """
UPSERT into analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets
SELECT bill_id, 
'01' as unusual_id,
company_code,
account_period,
account_item,
finance_number,
cost_center,
profit_center,
'' as cart_head,
bill_code,
bill_beg_date,
bill_end_date,
origin_name   as  origin_city,
destin_name  as destin_city,
travel_beg_date  as beg_date,
travel_end_date  as end_date,
'' as emp_name,
'' as emp_code,
jour_amount,
accomm_amount,
subsidy_amount,
other_amount,
check_amount,
jzpz,
'差旅费',
0 as meeting_amount
FROM 01_datamart_layer_007_h_cw_df.finance_travel_bill  
WHERE  billingdate is not null and travel_beg_date is not null and travel_end_date  is not null
and (unix_timestamp(billingdate, 'yyyy-MM-dd HH:mm:ss') < unix_timestamp(travel_beg_date,'yyyyMMdd')
or unix_timestamp(billingdate, 'yyyy-MM-dd HH:mm:ss') > unix_timestamp(travel_end_date,'yyyyMMdd'))  
group by bill_id,company_code,account_period,account_item,finance_number,cost_center,profit_center,bill_code,origin_name,
destin_name,travel_beg_date,travel_end_date,jour_amount,accomm_amount,subsidy_amount,other_amount,check_amount,jzpz,bill_beg_date,
bill_end_date
    """
    start_time = time.perf_counter()
    prod_execute_sql(sqltype='insert', sql=sql)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'*** 执行 check_01_invoice_data SQL耗时 {consumed_time} sec录')
    #dis_connection()


def check_02_trip_data():
    columns_ls = ['destin_name', 'sales_name', 'sales_addressphone', 'sales_bank']
    extra_columns_ls = ['bill_id', 'company_code', 'account_period', 'account_item', 'finance_number', 'cost_center',
                        'profit_center', 'bill_code', 'origin_name',
                        'destin_name', 'travel_beg_date', 'travel_end_date', 'jour_amount', 'accomm_amount',
                        'subsidy_amount', 'other_amount',
                        'check_amount', 'jzpz']
    columns_ls.extend(extra_columns_ls)
    columns_str = ",".join(columns_ls)

    sql = """
    select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill where destin_name is not null and (sales_name is not null and sales_addressphone is not null and sales_bank is not null ) limit 1000000
    """.format(columns_str=columns_str)
    start_time = time.perf_counter()
    select_sql_ls = []
    select_sql_ls.append(sql)
    query_data = []
    for sel_sql in select_sql_ls:
        # log.info(sel_sql)
        data = prod_execute_sql(sqltype='select', sql=sel_sql)
        # print(data)
        if data:
            query_data.extend(data)

    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* 查询耗时 {consumed_time} sec')
    print(len(data))

    match_query_ls = []
    for data in query_data:
        # print(data)
        destin_name = str(data[0])
        sales_name = str(data[1])
        sales_addressphone = str(data[2])
        sales_bank = str(data[3])

        is_match = False
        if sales_name != 'None' and sales_addressphone != 'None' and sales_bank != 'None':
            # print(destin_name , '| ',sales_name, sales_addressphone, sales_bank)

            # 只匹配市和县
            if sales_name != 'None':
                sales_name_city = match_address(place=sales_name, key='市') if match_address(place=sales_name,
                                                                                            key='市') else match_address(
                    place=sales_name, key='县')
                if sales_name_city != None:
                    if destin_name.find(sales_name_city) > -1:
                        is_match = True
                        continue

            if sales_addressphone != 'None':
                sales_addressphone_city = match_address(place=sales_addressphone, key='市') if match_address(
                    place=sales_addressphone, key='市') else match_address(place=sales_addressphone, key='县')
                if sales_addressphone_city != None:
                    if destin_name.find(sales_addressphone_city) > -1:
                        is_match = True
                        continue

            if sales_bank != 'None':
                sales_bank_city = match_address(place=sales_bank, key='市') if match_address(place=sales_bank,
                                                                                            key='市') else match_address(
                    place=sales_bank, key='县')
                if sales_bank_city != None:
                    if destin_name.find(sales_bank_city) > -1:
                        is_match = True
                        continue

            if is_match == False:
                match_query_ls.append(data)

    dest_file = "/my_filed_algos/check_02_trip_data2.json"
    if os.path.exists(dest_file):
        os.remove(dest_file)

    for item in match_query_ls:
        print(item)
        unusual_id = '02'
        bill_id = str(item[4]) if item[4] is not None else ''
        company_code = str(item[5]) if item[5] is not None else ''
        account_period = str(item[6]) if item[6] is not None else ''
        account_item = str(item[7]) if item[7] is not None else ''
        finance_number = str(item[8]) if item[8] is not None else ''
        cost_center = str(item[9]) if item[9] is not None else ''
        profit_center = str(item[10]) if item[10] is not None else ''
        cart_head = ' '
        bill_code = str(item[11]) if item[11] is not None else ''
        origin_city = str(item[12]) if item[12] is not None else ''
        destin_city = str(item[13]) if item[13] is not None else ''
        beg_date = str(item[14]) if item[14] is not None else ''
        end_date = str(item[15]) if item[15] is not None else ''
        emp_name = ' '
        emp_code = ' '
        jour_amount = item[16] if item[16] is not None else 0
        accomm_amount = item[17] if item[17] is not None else 0
        subsidy_amount = item[18] if item[18] is not None else 0
        other_amount = item[19] if item[19] is not None else 0
        check_amount = item[20] if item[20] is not None else 0
        jzpz = item[21] if item[21] is not None else 0

        dict_data = {'unusual_id': unusual_id, 'bill_id': bill_id, 'company_code': company_code,
                     'account_period': account_period, 'account_item': account_item,
                     'finance_number': finance_number, 'cost_center': cost_center, 'profit_center': profit_center,
                     'cart_head': cart_head, 'bill_code': bill_code,
                     'origin_city': origin_city, 'destin_city': destin_city, 'beg_date': beg_date, 'end_date': end_date,
                     'emp_name': emp_name, 'emp_code': emp_code,
                     'jour_amount': jour_amount, 'accomm_amount': accomm_amount, 'subsidy_amount': subsidy_amount,
                     'other_amount': other_amount, 'check_amount': check_amount,
                     'jzpz': jzpz}

        with open(dest_file, "a", encoding='utf-8') as file:
            json.dump(dict_data, file)
            file.write("\n")

    #dis_connection()
    print('-- ok ---')


def check_03_consistent_amount():
    start_time = time.perf_counter()
    sql = """
UPSERT into analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets 
select a.bill_id, 
'03' as unusual_id,
a.company_code,
a.account_period,
a.account_item,
a.finance_number,
a.cost_center,
a.profit_center,
'' as cart_head,
a.bill_code,
a.origin_name   as  origin_city,
a.destin_name  as destin_city,
a.travel_beg_date  as beg_date,
a.travel_end_date  as end_date,
'' as emp_name,
'' as emp_code,
a.jour_amount,
a.accomm_amount,
a.subsidy_amount,
a.other_amount,
a.check_amount,
a.jzpz,
'差旅费',
0 as meeting_amount
from (
	select bill_id,company_code,account_period,
		account_item,finance_number,cost_center,
		profit_center,bill_code,origin_name,destin_name,
		travel_beg_date,travel_end_date,jour_amount,
		accomm_amount,subsidy_amount,other_amount,
		check_amount,jzpz from (
	select 
		bill_id,company_code,account_period,
		account_item,finance_number,cost_center,
		profit_center,bill_code,origin_name,destin_name,
		travel_beg_date,travel_end_date,jour_amount,
		accomm_amount,subsidy_amount,other_amount,
		check_amount,jzpz,
		sum(jzpz) as sum_jzpz_amount
	from 01_datamart_layer_007_h_cw_df.finance_travel_bill
	group by bill_id,company_code,account_period,
		account_item,finance_number,cost_center,
		profit_center,bill_code,origin_name,destin_name,
		travel_beg_date,travel_end_date,jour_amount,
		accomm_amount,subsidy_amount,other_amount,
		check_amount,jzpz  
	) y where  check_amount > sum_jzpz_amount 
)a    
    """
    prod_execute_sql(sqltype='insert', sql=sql)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* 执行 check_03_consistent_amount SQL耗时 {consumed_time} sec')
    #dis_connection()


def check_04_overlap_amount():
    start_time = time.perf_counter()
    sql = """
UPSERT into analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets
select a.bill_id, 
'04' as unusual_id,
a.company_code,
a.account_period,
a.account_item,
a.finance_number,
a.cost_center,
a.profit_center,
'' as cart_head,
a.bill_code,
a.origin_name   as  origin_city,
a.destin_name  as destin_city,
a.travel_beg_date  as beg_date,
a.travel_end_date  as end_date,
'' as emp_name,
'' as emp_code,
a.jour_amount,
a.accomm_amount,
a.subsidy_amount,
a.other_amount,
a.check_amount,
a.jzpz,
'差旅费',
0 as meeting_amount
from  01_datamart_layer_007_h_cw_df.finance_travel_bill a, (
	select 
    jour_beg_date, 
    jour_end_date,
    member_emp_id, 
    bill_code,
    bill_id
	from (
		select 
			 y.jour_beg_date, 
			 y.jour_end_date,
			 y.member_emp_id, 
			 y.bill_code,
			 y.bill_id,
			 lead(y.jour_beg_date,1) over (partition by y.member_emp_id order by jour_beg_date) next_jour_beg_date,
			 lag(y.jour_end_date,1) over (partition by y.member_emp_id order by jour_beg_date) next_jour_end_date
		from  `01_datamart_layer_007_h_cw_df`.finance_travel_bill y 
		group by y.jour_beg_date, y.jour_end_date, y.member_emp_id, y.bill_code,y.bill_id
		having y.jour_beg_date is not null and y.jour_beg_date is not null
	) t1
	where jour_end_date>next_jour_beg_date
	or next_jour_end_date>jour_beg_date
) b
where a.bill_id = b.bill_id	
    """
    prod_execute_sql(sqltype='insert', sql=sql)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* 执行 check_04_overlap_amount SQL耗时 {consumed_time} sec')
    #dis_connection()


def check_06_reasonsubsidy_amount():
    """
    需求6： 通过按出差人天和报销标准，重新计算和复核出差补助报销金额，是否复核集团要求和规定，尤其是连续出差超过14天的，是否按照分段报销标准进行计算。
    :return:
    """

    # part1 select data
    start_time = time.perf_counter()
    # check_amount费用报销金额， jzpz 票据金额， 检查是否存在费用报销金额大于原始票据金额情况
    columns_ls = ['bill_id', 'apply_beg_date', 'apply_end_date', 'check_amount', 'total_date']
    sel_sql = """
    select a.bill_id, a.apply_beg_date, a.apply_end_date, a.check_amount,  a.total_date from
    (
     select bill_id, apply_beg_date, apply_end_date ,check_amount, (unix_timestamp(apply_end_date, 'yyyyMMdd')-unix_timestamp(apply_beg_date, 'yyyyMMdd'))/ (60 * 60 * 24) as total_date
     from 01_datamart_layer_007_h_cw_df.finance_travel_bill
    )a,(select standard_value, out_value from 01_datamart_layer_007_h_cw_df.finance_standard where unusual_id='06') b
    where  a.check_amount > ( 14 * b.standard_value + (a.total_date - 14 ) * b.out_value ) 
    and a.total_date > 14 limit 100
    """.replace('\r', '').replace('\n', '').strip()
    count_sql = 'select count(a.bill_id) from ({sql}) a'.format(sql=sel_sql)
    records = prod_execute_sql(sqltype='select', sql=count_sql)
    count_records = records[0][0]
    log.info(f'* 查询结果 {count_records} 条')

    max_size = 1 * 1000
    limit_size = 10000
    select_sql_ls = []
    sel_columns = []
    for column in columns_ls:
        sel_columns.append("a." + column)
    columns_str = ",".join(sel_columns)

    if count_records >= max_size:
        offset_size = 0
        while offset_size <= count_records:

            if offset_size + limit_size > count_records:
                limit_size = count_records - offset_size
                # sel_sql
                tmp_sql = "select {columns_str} from (sel_sql) order by bill_id limit {limit_size} offset {offset_size}".format(
                    columns_str=columns_str, limit_size=limit_size, offset_size=offset_size, sel_sql=sel_sql)

                # if limit_size != 0:
                select_sql_ls.append(tmp_sql)
                print('111 ', tmp_sql)
                break
            else:
                tmp_sql = "select {columns_str} from (sel_sql) order by bill_id limit {limit_size} offset {offset_size}".format(
                    columns_str=columns_str, limit_size=limit_size, offset_size=offset_size, sel_sql=sel_sql)
                select_sql_ls.append(tmp_sql)

            offset_size = offset_size + limit_size
    else:
        tmp_sql = "select {columns_str} from ({sel_sql})a".format(
            columns_str=columns_str, sel_sql=sel_sql)
        select_sql_ls.append(tmp_sql)

    # print(len(select_sql_ls), select_sql_ls)
    log.info('* 开始分页查询')
    for sql in select_sql_ls:
        print(sql)

    consumed_time = round(time.perf_counter() - start_time)
    print(f'* consumed_time={consumed_time} sec')

    #dis_connection()


def check_07_continuous_business_trip():
    start_time = time.perf_counter()
    sql = """
UPSERT into analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets
SELECT bill_id, 
'07' as unusual_id,
company_code,
account_period,
account_item,
finance_number,
cost_center,
profit_center,
'' as cart_head,
bill_code,
origin_name   as  origin_city,
destin_name  as destin_city,
travel_beg_date  as beg_date,
travel_end_date  as end_date,
'' as emp_name,
'' as emp_code,
jour_amount,
accomm_amount,
subsidy_amount,
other_amount,
check_amount,
jzpz,
'差旅费',
0 as meeting_amount
FROM 01_datamart_layer_007_h_cw_df.finance_travel_bill where bill_id in (
	select 
	    bill_id
	from (
		select 
		   t.bill_id,
		   t.destin_name,
		   t.jour_beg_date,
		   t.jour_end_date,
		   lead(t.jour_beg_date,1) over (partition by t.destin_name order by t.jour_beg_date) next_jour_beg_date,
		   lag(t.jour_end_date,1) over (partition by t.destin_name order by t.jour_beg_date) before_jour_end_date
		from  `01_datamart_layer_007_h_cw_df`.finance_travel_bill t
		where t.destin_name is not null and  t.jour_beg_date is not null 
			 and  t.jour_end_date is not null
	) t
	where
		datediff(
			concat(left(next_jour_beg_date,4),'-',substr(next_jour_beg_date,5,2),'-',right(next_jour_beg_date,2)),
			concat(left(jour_end_date,4),'-',substr(jour_end_date,5,2),'-',right(jour_end_date,2))
		) <= 2
		or
		datediff(
			concat(left(jour_beg_date,4),'-',substr(jour_beg_date,5,2),'-',right(jour_beg_date,2)),
			concat(left(before_jour_end_date,4),'-',substr(before_jour_end_date,5,2),'-',right(before_jour_end_date,2))
		) <= 2
)
    """
    prod_execute_sql(sqltype='insert', sql=sql)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* 执行check_07_continuous_business_trip SQL耗时 {consumed_time} sec')
    #dis_connection()


def pre_check_08_transportation():
    """
    7499
    运算结果：4548

    """
    org_id = '20020001'
    result = queryByParent_check08(parent_id=org_id)
    orgid_ls = []
    pre_check_08_data(orgid_ls, org_id, result)
    print(len(orgid_ls))  # 4548
    print(orgid_ls[0:5])


def pre_check_08_data(orgid_ls, id, result):
    sub_orgid_ls = []
    for item in result:
        org_id = str(item[0])
        parent_id = str(item[1])
        #print('111 ', item)

        if parent_id == id:
            sub_orgid_ls.append(org_id)
            #orgid_ls.append(org_id)

    if len(sub_orgid_ls) > 0:
        orgid_ls.extend(sub_orgid_ls)

    for sub_orgid in sub_orgid_ls:
        result = queryByParent_check08(parent_id=sub_orgid)
        if result and len(result) > 0 :
            print('222 ',result)
            pre_check_08_data(orgid_ls, sub_orgid, result)
        elif result is None:
            return


def queryByParent_check08(parent_id='20020001'):
    """
    处理需求8 需要的数据
    :param org_id:
    :return:
    """
    sql = f"select org_id,parent_id,org_name from 03_basal_layer_zfybxers00.zfybxers00_z_mdm_organization where parent_id = '{parent_id}'"
    #log.info(sql)
    result = prod_execute_sql(sqltype='select', sql=sql)
    # print(len(result),result)

    if len(result) > 0:
        return result
    else:
        return None


def check_08_transportation_expenses():
    start_time = time.perf_counter()
    sql = """


    """
    prod_execute_sql(sqltype='insert', sql=sql)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* check_08_transportation_expenses SQL耗时 {consumed_time} sec')
    #dis_connection()


def check_10_beforeapply_amount():
    """
    需求10： 报销申请单不存在关联的事前申请单号，或者事前申请的日期晚于出差开始日期（即差旅行程的出发日期）
    :return:
    """

    start_time = time.perf_counter()
    sql = """
UPSERT into analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets
SELECT bill_id, 
'10' as unusual_id,
company_code,
account_period,
account_item,
finance_number,
cost_center,
profit_center,
'' as cart_head,
bill_code,
origin_name   as  origin_city,
destin_name  as destin_city,
travel_beg_date  as beg_date,
travel_end_date  as end_date,
'' as emp_name,
'' as emp_code,
jour_amount,
accomm_amount,
subsidy_amount,
other_amount,
check_amount,
jzpz,
'差旅费',
0 as meeting_amount
FROM 01_datamart_layer_007_h_cw_df.finance_travel_bill 
WHERE apply_id='' or unix_timestamp(base_apply_date, 'yyyyMMdd') > unix_timestamp(jour_beg_date, 'yyyyMMdd')
group by bill_id,company_code,account_period,account_item,finance_number,cost_center,profit_center,bill_code,origin_name,
destin_name,travel_beg_date,travel_end_date,jour_amount,accomm_amount,subsidy_amount,other_amount,check_amount,jzpz    
        """
    prod_execute_sql(sqltype='insert', sql=sql)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* 执行 check_10_beforeapply_amount SQL耗时 {consumed_time} sec')
    #dis_connection()


def check_13_accommodation_price():
    columns_ls = ['finance_travel_id', 'bill_id', 'apply_emp_id', 'bill_code', 'bill_type_code', 'bill_type_name',
                  'apply_emp_name', 'bill_apply_date', 'check_amount', 'bill_remark',
                  'bill_beg_date', 'bill_end_date', 'travel_bill_id', 'travel_aim_name', 'travel_country_name',
                  'travel_region_name', 'travel_city_name', 'travel_beg_date', 'travel_end_date',
                  'apply_id', 'apply_code', 'cost_cent_sname', 'base_apply_date', 'base_currcy_amount', 'base_remark',
                  'b_currcy_name', 'base_beg_date', 'base_end_date',
                  'travel_apply_id', 'apply_aim_name', 'apply_country_name', 'apply_region_name', 'apply_city_name',
                  'apply_beg_date', 'apply_end_date', 'bill_apply_id', 'rebill_apply_id',
                  'creater', 'member_bill_id', 'member_emp_id', 'member_emp_name', 'invo_code', 'invo_number',
                  'invoice_type_name', 'billingdate', 'sales_name', 'sales_addressphone',
                  'sales_bank', 'origin_name', 'jour_amount', 'jour_beg_date', 'jour_end_date', 'destin_name',
                  'accomm_amount', 'subsidy_amount', 'other_amount', 'jzpz', 'company_code', 'account_period',
                  'finance_number', 'cost_center', 'profit_center', 'account_item', 'exec_name', 'arrivedtimes']
    columns_str = ",".join(columns_ls)

    sql = """
        select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill limit 100000
            """.format(columns_str=columns_str)
    count_sql = 'select count(a.bill_id) from ({sql}) a'.format(sql=sql)
    # log.info(count_sql)
    records = prod_execute_sql(sqltype='select', sql=count_sql)
    count_records = records[0][0]
    log.info(f'* count_records={count_records}')

    start_time = time.perf_counter()
    log.info('* 开始查询')
    select_sql_ls = []

    select_sql_ls.append(sql)
    query_data = []
    for sel_sql in select_sql_ls:
        # log.info(sel_sql)
        data = prod_execute_sql(sqltype='select', sql=sel_sql)
        # print(data)
        if data:
            query_data.extend(data)

    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'*** 查询耗时 {consumed_time} sec, 共有 {len(query_data)} 条记录')

    create_finance_travel_bill_csv(columns_ls, query_data)

def check_21_credit():
    start_time = time.perf_counter()
    sql = """
    UPSERT into analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets
select bill_id, 
    '21' as unusual_id,
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
    '差旅费',
    0 as meeting_amount
  from 01_datamart_layer_007_h_cw_df.finance_travel_bill where account_period !='NULL'  and  
(cast(CONCAT(substr(account_period,1,4),'',substr(substr(account_period,5,7),2,3)) as int ) > cast(substr(arrivedtimes,1,7) as int))
                """
    prod_execute_sql(sqltype='insert', sql=sql)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* 执行 check_21_credit SQL耗时 {consumed_time} sec')
    #dis_connection()

def check_22_apply():
    start_time = time.perf_counter()
    sql = """
UPSERT into analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets
select bill_id, 
    '22' as unusual_id,
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
    '差旅费',
    0 as meeting_amount from 
01_datamart_layer_007_h_cw_df.finance_travel_bill
where
jour_end_date is not null
and 
tb_times is not null 
and
jour_end_date>tb_times
            """
    prod_execute_sql(sqltype='insert', sql=sql)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* 执行 check_22_apply SQL耗时 {consumed_time} sec')
    #dis_connection()

def check_23_apply():
    start_time = time.perf_counter()
    sql = """
    UPSERT into analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets
with standard as (
  select standard_value from 01_datamart_layer_007_h_cw_df.finance_standard where unusual_id='23'
)
select bill_id, 
    '23' as unusual_id,
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
    '差旅费',
    0 as meeting_amount from 
01_datamart_layer_007_h_cw_df.finance_travel_bill as travel_bill,standard
where
arrivedtimes is not null
and 
tb_times is not null 
and
((cast(replace(substr(arrivedtimes,1,7),'-','') as int)-cast(replace(substr(tb_times,1,7),'-','')  as int))-(select avg(cast(replace(substr(arrivedtimes,1,7),'-','')  as int)-cast(replace(substr(tb_times,1,7),'-','')  as int)) avgt from 
01_datamart_layer_007_h_cw_df.finance_travel_bill))>standard.standard_value
            """
    prod_execute_sql(sqltype='insert', sql=sql)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* 执行 check_23_apply SQL耗时 {consumed_time} sec')
    #dis_connection()



def create_finance_travel_bill_csv(columns_ls, query_data):
    # 创建 csv 文件
    out = open('/my_filed_algos/finance_travel_bill.csv', 'a', newline='', encoding='utf-8')
    csv_writer = csv.writer(out, dialect='excel')
    csv_writer.writerow(columns_ls)

    for data in query_data:
        csv_writer.writerow(data)

    log.info("* write csv over")


def check_15_coststructure_data():
    start_time = time.perf_counter()
    sql = """
    UPSERT into analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets
    SELECT bill_id, 
    '15' as unusual_id,
    company_code,
    account_period,
    account_item,
    finance_number,
    cost_center,
    profit_center,
    '' as cart_head,
    bill_code,
    origin_name   as  origin_city,
    destin_name  as destin_city,
    travel_beg_date  as beg_date,
    travel_end_date  as end_date,
    '' as emp_name,
    '' as emp_code,
    jour_amount,
    accomm_amount,
    subsidy_amount,
    other_amount,
    check_amount,
    jzpz,
    '差旅费',
    0 as meeting_amount
    FROM 01_datamart_layer_007_h_cw_df.finance_travel_bill  
    WHERE accomm_amount=0 or jour_amount=0
    group by bill_id,company_code,account_period,account_item,finance_number,cost_center,profit_center,bill_code,origin_name,
    destin_name,travel_beg_date,travel_end_date,jour_amount,accomm_amount,subsidy_amount,other_amount,check_amount,jzpz  
            """
    prod_execute_sql(sqltype='insert', sql=sql)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* 执行 check_15_coststructure_data SQL耗时 {consumed_time} sec')
    #dis_connection()


def get_travel_jiebaword():
    """
    抽取差旅费的关键字
    :return:
    """

    sql = "select distinct commodityname from 01_datamart_layer_007_h_cw_df.finance_travel_bill where commodityname is not null and commodityname !='' "
    records = prod_execute_sql(conn_type='test', sqltype='select', sql=sql)
    jiebaword = []
    words = []
    for record in records:
        record_str = str(record[0])

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


if __name__ == "__main__":
    #main()
    keywords = get_travel_jiebaword()
    for keyword in keywords:
        print(keyword)
