# -*- coding: utf-8 -*-
"""
Created on 2021-08-05

@author: WangShuo
"""

from report.commons.logging import get_logger
from report.commons.connect_kudu import prod_execute_sql, dis_connection
import time

log = get_logger(__name__)


def check_03_consistent_amount():
    columns_ls = ['company_code', 'bill_id', 'account_period', 'account_item', 'finance_number', 'cost_center',
                  'profit_center', 'bill_code', 'origin_name', 'destin_name', 'travel_beg_date', 'travel_end_date',
                  'jour_amount', 'accomm_amount', 'subsidy_amount', 'other_amount',
                  'apply_emp_id', 'apply_emp_name', 'check_amount', 'jzpz']
    columns_str = ",".join(columns_ls)

    # part1: select data
    start_time = time.perf_counter()
    sql = 'select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill where check_amount > jzpz limit 100000'.format(
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
check_amount,  jzpz, unusual_id)VALUES
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
    for insert_sql in sqllist:
        # print(insert_sql)
        prod_execute_sql(sqltype='insert', sql=insert_sql)

    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* 插入耗时 {consumed_time} sec')


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
    log.info(f'* 查询结果 {count_records} 条' )

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
                    columns_str=columns_str, limit_size=limit_size, offset_size=offset_size,sel_sql=sel_sql)

                #if limit_size != 0:
                select_sql_ls.append(tmp_sql)
                print('111 ',tmp_sql)
                break
            else:
                tmp_sql = "select {columns_str} from (sel_sql) order by bill_id limit {limit_size} offset {offset_size}".format(
                    columns_str=columns_str, limit_size=limit_size, offset_size=offset_size,sel_sql=sel_sql)
                select_sql_ls.append(tmp_sql)

            offset_size = offset_size + limit_size
    else:
        tmp_sql = "select {columns_str} from ({sel_sql})a".format(
            columns_str=columns_str,sel_sql=sel_sql)
        select_sql_ls.append(tmp_sql)

    # print(len(select_sql_ls), select_sql_ls)
    log.info('* 开始分页查询')
    for sql in select_sql_ls:
        print(sql)



    consumed_time = round(time.perf_counter() - start_time)
    print(f'* consumed_time={consumed_time} sec')

    dis_connection()


if __name__ == "__main__":
    # 需求3
    # check_03_consistent_amount()

    # 需求6
    check_06_reasonsubsidy_amount()
