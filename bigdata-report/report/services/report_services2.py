# -*- coding: utf-8 -*-
"""
Created on 2021-08-05

@author: WangShuo
"""

from report.commons.logging import get_logger
from report.commons.connect_kudu import prod_execute_sql, dis_connection
from report.commons.tools import match_address
import time
import json
import os

log = get_logger(__name__)


def demo():
    # del_sql = 'delete from 01_datamart_layer_007_h_cw_df.finance_all_targets  where unusual_id="10" '
    # prod_execute_sql(sqltype='insert', sql=del_sql)

    # sel_sql = "select count(bill_id) from 01_datamart_layer_007_h_cw_df.finance_all_targets where unusual_id='10' "
    # records = prod_execute_sql(sqltype='select', sql=sel_sql)
    # count_records = records[0][0]
    # log.info(f'查询记录总数 {count_records}')

    sel_sql ="""
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
    #demo()

    # 需求1
    #check_01_invoice_data()

    # 需求2 未做
    check_02_trip_data()

    # 需求3
    #check_03_consistent_amount()

    # 需求6 暂时不做
    # check_06_reasonsubsidy_amount()

    # 需求10 ???
    #check_10_beforeapply_amount()

    # 需求15
    #check_15_coststructure_data()


def check_01_invoice_data():
    columns_ls = ['bill_id', 'invo_code', 'billingdate', 'travel_beg_date', 'travel_end_date']
    add_columns_ls = ['company_code', 'account_period', 'account_item', 'finance_number', 'cost_center', 'bill_code',
                      'origin_name', 'destin_name',
                      'jour_amount', 'accomm_amount', 'subsidy_amount', 'other_amount', 'check_amount', 'jzpz']
    columns_ls.extend(add_columns_ls)
    columns_str = ",".join(columns_ls)

    # part1: select data
    start_time = time.perf_counter()
    sql = """
    select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill  
    where  billingdate is not null and travel_beg_date is not null and travel_end_date  is not null
    and (unix_timestamp(billingdate, 'yyyy-MM-dd HH:mm:ss')< unix_timestamp(travel_beg_date,'yyyyMMdd')
     or unix_timestamp(billingdate, 'yyyy-MM-dd HH:mm:ss')> unix_timestamp(travel_end_date,'yyyyMMdd'))
    group by {columns_str}
        """.format(columns_str=columns_str)

    log.info(sql)

    count_sql = 'select count(a.bill_id) from ({sql}) a'.format(sql=sql)
    # log.info(count_sql)
    records = prod_execute_sql(sqltype='select', sql=count_sql)
    count_records = records[0][0]
    log.info(f'* count_records={count_records}')

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

    # part2 insert data
    start_time = time.perf_counter()
    batch_size = 10000
    sqllist = []
    batch_list = []
    idx = 0
    tmp_ls = []
    insert_sql = """
    INSERT INTO table 01_datamart_layer_007_h_cw_df.finance_all_targets(bill_id, unusual_id,beg_date, end_date, company_code, account_period, account_item, finance_number, cost_center, bill_code, origin_city, destin_city, jour_amount, accomm_amount, subsidy_amount, other_amount, check_amount, jzpz) VALUES
            """.replace('\r', '').replace('\n', '').strip()

    for item in query_data:
        idx += 1
        # print(item)

        bill_id = item[0] if item[0] is not None else ''
        unusual_id = '01'
        beg_date = item[3] if item[3] is not None else ''
        end_date = item[4] if item[4] is not None else ''
        company_code = item[5] if item[5] is not None else ''
        account_period = item[6] if item[6] is not None else ''
        account_item = item[7] if item[7] is not None else ''
        finance_number = item[8] if item[8] is not None else ''
        cost_center = item[9] if item[9] is not None else ''
        bill_code = item[10] if item[10] is not None else ''
        origin_city = item[11] if item[11] is not None else ''
        destin_city = item[12] if item[12] is not None else ''
        jour_amount = item[13] if item[13] is not None else 0
        accomm_amount = item[14] if item[14] is not None else 0
        subsidy_amount = item[15] if item[15] is not None else 0
        other_amount = item[16] if item[16] is not None else 0

        check_amount = item[17] if item[17] is not None else 0
        jzpz = item[18] if item[18] is not None else 0

        value_sql = f"""(
 "{bill_id}","{unusual_id}" ,"{beg_date}","{end_date}", "{company_code}", "{account_period}", "{account_item}", "{finance_number}", "{cost_center}", "{bill_code}",
 "{origin_city}", "{destin_city}" ,{jour_amount}, {accomm_amount},{subsidy_amount},{other_amount}, {check_amount}, {jzpz} )
""".replace('\r', '').replace('\n', '').strip()

        if idx < batch_size:
            tmp_ls.append(value_sql)
        elif idx == batch_size:
            # 满一个批次进行处理操作
            tmp_ls.append(value_sql)
            result_sql = insert_sql + ",".join(tmp_ls)
            sqllist.append(result_sql)
            # print('* inner batch tmp_ls=', ','.join(tmp_ls))
            tmp_ls.clear()
            idx = 0

    if tmp_ls:
        # print('* outer batch tmp_ls=', ','.join(tmp_ls))
        result_sql = insert_sql + ",".join(tmp_ls)
        sqllist.append(result_sql)

    log.info(f'* ready sql for insert, {len(sqllist)} 个批处理SQL')
    # for insert_sql in sqllist:
    #     # print(insert_sql)
    #     try:
    #         prod_execute_sql(sqltype='insert', sql=insert_sql)
    #     except Exception as e:
    #         print(e)
    #         break
    #
    # consumed_time = round(time.perf_counter() - start_time)
    # log.info(f'* 插入耗时 {consumed_time} sec')

    dis_connection()


def check_02_trip_data():
    columns_ls = ['destin_name', 'sales_name', 'sales_addressphone', 'sales_bank' ]
    extra_columns_ls = ['bill_id', 'company_code', 'account_period', 'account_item','finance_number','cost_center','profit_center','bill_code', 'origin_name',
                        'destin_name', 'travel_beg_date' , 'travel_end_date', 'jour_amount', 'accomm_amount', 'subsidy_amount', 'other_amount',
                        'check_amount', 'jzpz' ]
    columns_ls.extend(extra_columns_ls)
    columns_str = ",".join(columns_ls)

    sql = """
    select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill where destin_name is not null limit 1000000
    """.format(columns_str=columns_str)
    start_time = time.perf_counter()
    select_sql_ls= []
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
        #print(data)
        destin_name = str(data[0])
        sales_name = str(data[1])
        sales_addressphone = str(data[2])
        sales_bank = str(data[3])

        is_match = False
        if sales_name != 'None' and sales_addressphone != 'None' and sales_bank != 'None':
            #print(destin_name , '| ',sales_name, sales_addressphone, sales_bank)

            # 只匹配市和县
            if sales_name != 'None':
                sales_name_city = match_address(place=sales_name,key='市') if match_address(place=sales_name,key='市') else match_address(place=sales_name,key='县')
                if sales_name_city != None:
                    if destin_name.find(sales_name_city) > -1:
                        is_match = True
                        break

            if sales_addressphone != 'None':
                sales_addressphone_city = match_address(place=sales_addressphone,key='市') if match_address(place=sales_addressphone,key='市') else match_address(place=sales_addressphone,key='县')
                if sales_addressphone_city != None:
                    if destin_name.find(sales_addressphone_city) > -1:
                        is_match = True
                        break

            if sales_bank != 'None':
                sales_bank_city = match_address(place=sales_bank,key='市') if match_address(place=sales_bank,key='市') else match_address(place=sales_bank,key='县')
                if sales_bank_city != None:
                    if destin_name.find(sales_bank_city) > -1:
                        is_match = True
                        break

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

        dict_data = {'unusual_id' : unusual_id, 'bill_id' : bill_id, 'company_code': company_code, 'account_period' : account_period , 'account_item' : account_item,
                     'finance_number' : finance_number, 'cost_center': cost_center, 'profit_center' : profit_center, 'cart_head':cart_head, 'bill_code' : bill_code,
                     'origin_city' : origin_city, 'destin_city' : destin_city,  'beg_date' : beg_date, 'end_date' : end_date , 'emp_name' : emp_name, 'emp_code':emp_code,
                     'jour_amount' : jour_amount, 'accomm_amount' : accomm_amount, 'subsidy_amount': subsidy_amount, 'other_amount': other_amount,'check_amount':check_amount,
                     'jzpz': jzpz}


        with open(dest_file, "a", encoding='utf-8') as file:
            json.dump(dict_data, file)
            file.write("\n")

    dis_connection()
    print('-- ok ---')



def check_03_consistent_amount():
    columns_ls = ['company_code', 'bill_id', 'account_period', 'account_item', 'finance_number', 'cost_center',
                  'profit_center', 'bill_code', 'origin_name', 'destin_name', 'travel_beg_date', 'travel_end_date',
                  'jour_amount', 'accomm_amount', 'subsidy_amount', 'other_amount',
                  'apply_emp_id', 'apply_emp_name', 'check_amount', 'jzpz']
    columns_str = ",".join(columns_ls)

    # part1: select data
    start_time = time.perf_counter()
    sql = 'select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill where check_amount > jzpz limit 1000'.format(
        columns_str=columns_str)
    count_sql = 'select count(a.bill_id) from ({sql}) a'.format(sql=sql)

    records = prod_execute_sql(sqltype='select', sql=count_sql)
    count_records = records[0][0]
    max_size = 1 * 1000
    limit_size = 10000
    select_sql_ls = []

    log.info('* count_records={count_records}'.format(count_records=count_records))
    if count_records >= max_size:
        offset_size = 0
        while offset_size <= count_records:

            if offset_size + limit_size > count_records:
                limit_size = count_records - offset_size
                tmp_sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill order by bill_id limit {limit_size} offset {offset_size}".format(
                    columns_str=columns_str, limit_size=limit_size, offset_size=offset_size)

                if limit_size != 0:
                    select_sql_ls.append(tmp_sql)
                break
            else:
                tmp_sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill order by bill_id limit {limit_size} offset {offset_size}".format(
                    columns_str=columns_str, limit_size=limit_size, offset_size=offset_size)
                select_sql_ls.append(tmp_sql)

            offset_size = offset_size + limit_size
    else:
        tmp_sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill".format(
            columns_str=columns_str)
        select_sql_ls.append(tmp_sql)

    # print(len(select_sql_ls), select_sql_ls)
    log.info('* 开始分页查询')

    query_data = []
    for sel_sql in select_sql_ls:
        # log.info(sel_sql)
        data = prod_execute_sql(sqltype='select', sql=sel_sql)
        # print(data)
        if data:
            query_data.extend(data)

    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* 查询耗时 {consumed_time} sec')

    # part2 insert data
    # print(len(query_data))

    batch_size = 10000
    sqllist = []
    tmp_ls = []
    insert_sql = """
    INSERT INTO table 01_datamart_layer_007_h_cw_df.finance_all_targets(company_code, bill_id, account_period, 
account_item , finance_number ,cost_center, 
profit_center, cart_head, bill_code, 
origin_city, destin_city, beg_date, end_date,
jour_amount, accomm_amount, 
subsidy_amount, other_amount,
emp_code,emp_name, 
check_amount,  jzpz, unusual_id) VALUES
    """.replace('\r', '').replace('\n', '').strip()
    idx = 0
    start_time = time.perf_counter()

    for item in query_data:
        idx += 1

        company_code = item[0] if item[0] is not None else ''
        bill_id = item[1] if item[1] is not None else ''
        account_period = item[2] if item[2] is not None else ''
        account_item = item[3] if item[3] is not None else ''
        finance_number = item[4] if item[4] is not None else ''
        cost_center = item[5] if item[5] is not None else ''
        profit_center = item[6] if item[6] is not None else ''
        cart_head = ''
        bill_code = item[7] if item[7] is not None else ''
        origin_city = item[8] if item[8] is not None else ''
        destin_city = item[9] if item[9] is not None else ''
        beg_date = item[10] if item[10] is not None else ''
        end_date = item[11] if item[11] is not None else ''
        jour_amount = item[12] if item[12] is not None else 0
        accomm_amount = item[13] if item[13] is not None else 0
        subsidy_amount = item[14] if item[14] is not None else 0
        other_amount = item[15] if item[15] is not None else 0
        emp_code = item[16] if item[16] is not None else ''
        emp_name = item[17] if item[17] is not None else ''
        check_amount = item[18]
        jzpz = item[19]
        unusual_id = '03'

        value_sql = """(
"{company_code}", "{bill_id}" ,  "{account_period}" , 
"{account_item}" ,"{finance_number}", "{cost_center}", 
"{profit_center}", "{cart_head}", "{bill_code}" , 
"{origin_city}", "{destin_city}", "{beg_date}", "{end_date}",                            
{jour_amount}, {accomm_amount},    
{other_amount}, {other_amount},                        
"{emp_code}", "{emp_name}" , 
{check_amount} , {jzpz} , "{unusual_id}" )
        """.format(company_code=company_code, bill_id=bill_id,
                   account_period=account_period, account_item=account_item,
                   finance_number=finance_number, cost_center=cost_center,
                   profit_center=profit_center, cart_head=cart_head, bill_code=bill_code,
                   origin_city=origin_city, destin_city=destin_city, beg_date=beg_date,
                   end_date=end_date,
                   jour_amount=jour_amount, accomm_amount=accomm_amount,
                   subsidy_amount=subsidy_amount, other_amount=other_amount,
                   emp_code=emp_code, emp_name=emp_name,
                   check_amount=check_amount, jzpz=jzpz, unusual_id=unusual_id).replace('\r', '').replace('\n',
                                                                                                          '').strip()

        if idx < batch_size:
            tmp_ls.append(value_sql)
        elif idx == batch_size:
            # 满一个批次进行处理操作
            tmp_ls.append(value_sql)
            result_sql = insert_sql + ",".join(tmp_ls)
            sqllist.append(result_sql)
            # print('* inner batch tmp_ls=', ','.join(tmp_ls))
            tmp_ls.clear()
            idx = 0

    if tmp_ls:
        # print('* outer batch tmp_ls=', ','.join(tmp_ls))
        result_sql = insert_sql + ",".join(tmp_ls)
        sqllist.append(result_sql)

    log.info('* ready sql for insert')
    # for insert_sql in sqllist:
    #     print(insert_sql)
    #     prod_execute_sql(sqltype='insert', sql=insert_sql)

    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* 插入耗时 {consumed_time} sec')
    dis_connection()


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
    and a.total_date >14 limit 1020
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

    dis_connection()


def check_10_beforeapply_amount():
    """
    需求10： 报销申请单不存在关联的事前申请单号，或者事前申请的日期晚于出差开始日期（即差旅行程的出发日期）
    :return:
    """

    columns_ls = [ 'bill_id', 'company_code','account_period', 'account_item', 'finance_number', 'cost_center',
                  'profit_center', 'bill_code', 'origin_name', 'destin_name', 'travel_beg_date', 'travel_end_date',
                  'jour_amount', 'accomm_amount', 'subsidy_amount', 'other_amount',
                  'apply_emp_id', 'apply_emp_name', 'check_amount', 'jzpz' ]
    columns_str = ",".join(columns_ls)
    select_sql_ls = []

    # part1 select data
    start_time = time.perf_counter()
    sql = """
select {columns_str} from  01_datamart_layer_007_h_cw_df.finance_travel_bill where 
apply_id='' or unix_timestamp(base_apply_date, 'yyyyMMdd') > unix_timestamp(jour_beg_date, 'yyyyMMdd')
group by {columns_str}
    """.format(columns_str=columns_str)

    log.info(sql)
    count_sql = 'select count(a.bill_id) from ({sql}) a'.format(sql=sql)

    records = prod_execute_sql(conn_type='prod',sqltype='select', sql=count_sql)
    count_records = records[0][0]
    log.info('* count_records={count_records}'.format(count_records=count_records))
    select_sql_ls.append(sql)
    log.info('* 开始分页查询')

    query_data = []
    for sel_sql in select_sql_ls:
        # log.info(sel_sql)
        data = prod_execute_sql(sqltype='select', sql=sel_sql)
        # print(data)
        if data:
            query_data.extend(data)

    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* 查询耗时 {consumed_time} sec')

    # part2 insert data
    batch_size = 10000
    sqllist = []
    tmp_ls = []
    insert_sql = """
        INSERT INTO table 01_datamart_layer_007_h_cw_df.finance_all_targets(unusual_id, bill_id, company_code, account_period ) VALUES
        """.replace('\r', '').replace('\n', '').strip()
    idx = 0
    start_time = time.perf_counter()

    for item in query_data:
        idx += 1
        #print(item)
        unusual_id = '10'
        bill_id = item[0] if item[0] is not None else ''
        company_code = item[1] if item[1] is not None else ''
        account_period = item[2] if item[2] is not None else ''

        value_sql = f"""(
 "{unusual_id}", "{bill_id}", "{company_code}" ,"{account_period}" )
        """.replace('\r', '').replace('\n', '').strip()

        if idx < batch_size:
            tmp_ls.append(value_sql)
        elif idx == batch_size:
            # 满一个批次进行处理操作
            tmp_ls.append(value_sql)
            result_sql = insert_sql + ",".join(tmp_ls)
            sqllist.append(result_sql)
            # print('* inner batch tmp_ls=', ','.join(tmp_ls))
            tmp_ls.clear()
            idx = 0

    if tmp_ls:
        # print('* outer batch tmp_ls=', ','.join(tmp_ls))
        result_sql = insert_sql + ",".join(tmp_ls)
        sqllist.append(result_sql)

    log.info('* ready sql for insert')
    # for insert_sql in sqllist:
    #     #print(insert_sql)
    #     prod_execute_sql(sqltype='insert', sql=insert_sql)
    #
    # consumed_time = round(time.perf_counter() - start_time)
    # log.info(f'* 插入耗时 {consumed_time} sec')

    dis_connection()


def check_15_coststructure_data():
    start_time = time.perf_counter()
    columns_ls = ['finance_travel_id', 'bill_id', 'bill_apply_id', 'accomm_amount', 'jour_amount']
    columns_str = ",".join(columns_ls)

    sql = "select bill_id, bill_apply_id, accomm_amount, jour_amount from 01_datamart_layer_007_h_cw_df.finance_travel_bill where accomm_amount=0 or jour_amount=0 limit 1310"
    count_sql = 'select count(a.bill_id) from ({sql}) a'.format(sql=sql)
    records = prod_execute_sql(sqltype='select', sql=count_sql)
    count_records = records[0][0]
    max_size = 1 * 1000
    limit_size = 500
    select_sql_ls = []

    log.info(f'* 查询结果 count_records={count_records}')

    consumed_time = round(time.perf_counter() - start_time)
    print(f'* query records consumed_time={consumed_time} sec')

    if count_records >= max_size:
        offset_size = 0
        while offset_size <= count_records:

            if offset_size + limit_size > count_records:
                limit_size = count_records - offset_size
                tmp_sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill where accomm_amount=0 or jour_amount=0 order by bill_id limit {limit_size} offset {offset_size}".format(
                    columns_str=columns_str, limit_size=limit_size, offset_size=offset_size)

                if limit_size != 0:
                    select_sql_ls.append(tmp_sql)
                break
            else:
                tmp_sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill where accomm_amount=0 or jour_amount=0 order by bill_id limit {limit_size} offset {offset_size}".format(
                    columns_str=columns_str, limit_size=limit_size, offset_size=offset_size)
                select_sql_ls.append(tmp_sql)

            offset_size = offset_size + limit_size
    else:
        tmp_sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill where accomm_amount=0 or jour_amount=0".format(
            columns_str=columns_str)
        select_sql_ls.append(tmp_sql)

    log.info('* 开始分页查询')
    query_data = []
    for sel_sql in select_sql_ls:
        # log.info(sel_sql)
        data = prod_execute_sql(sqltype='select', sql=sel_sql)
        # print(data)
        if data:
            query_data.extend(data)

    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* 查询耗时 {consumed_time} sec')

    # part2 insert data
    batch_size = 500
    sqllist = []
    tmp_ls = []
    insert_sql = """
    INSERT INTO table 01_datamart_layer_007_h_cw_df.finance_travel_bill(bill_id, bill_apply_id, accomm_amount, jour_amount，unusual_id) VALUES
    """.replace('\r', '').replace('\n', '').strip()
    idx = 0
    start_time = time.perf_counter()

    print(insert_sql)
    for item in query_data:
        idx += 1
        # print(item)
        bill_id = item[0] if item[0] is not None else ''
        bill_apply_id = item[1] if item[1] is not None else ''
        accomm_amount = item[2] if item[2] is not None else 0
        jour_amount = item[3] if item[3] is not None else 0
        unusual_id = '15'
        # print(bill_id, bill_apply_id,accomm_amount, jour_amount)

        value_sql = """(
"{bill_id}","{bill_apply_id}", "{accomm_amount}", {jour_amount}, {unusual_id} )
""".format(bill_id=bill_id, bill_apply_id=bill_apply_id, accomm_amount=accomm_amount,
           jour_amount=jour_amount, unusual_id=unusual_id).replace('\r', '').replace('\n', '').strip()

        if idx < batch_size:
            tmp_ls.append(value_sql)
        elif idx == batch_size:
            # 满一个批次进行处理操作
            tmp_ls.append(value_sql)
            result_sql = insert_sql + ",".join(tmp_ls)
            sqllist.append(result_sql)
            # print('* inner batch tmp_ls=', ','.join(tmp_ls))
            tmp_ls.clear()
            idx = 0

    if tmp_ls:
        # print('* outer batch tmp_ls=', ','.join(tmp_ls))
        result_sql = insert_sql + ",".join(tmp_ls)
        sqllist.append(result_sql)

    log.info('* ready sql for insert')
    for insert_sql in sqllist:
        print(insert_sql)
        # prod_execute_sql(sqltype='insert', sql=insert_sql)

    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* 插入耗时 {consumed_time} sec')
    dis_connection()


if __name__ == "__main__":
    main()
