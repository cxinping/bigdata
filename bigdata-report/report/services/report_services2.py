# -*- coding: utf-8 -*-
"""
Created on 2021-08-05

@author: WangShuo
"""

from report.commons.logging import get_logger
import pandas as pd
from report.commons.connect_kudu import prod_execute_sql, dis_connection
import time

pd.set_option('display.max_columns', None)  # 显示完整的列
pd.set_option('display.max_rows', None)  # 显示完整的行
pd.set_option('display.expand_frame_repr', False)  # 设置不折叠数据

log = get_logger(__name__)


def query_kudu_data(sql, columns):
    """
    发票日期异常检查
    :return:
    """
    records = prod_execute_sql(sqltype='select', sql=sql)
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
                else:
                    record.append(str(item[idx]))

        dataFromKUDU.append(record)

    df = pd.DataFrame(data=dataFromKUDU, columns=columns)
    return df


def check_03_consistent_amount_222():
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

                if limit_size != 0 :
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
        #log.info(sel_sql)
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
    """.replace('\r','').replace('\n','').strip()
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
                                               check_amount=check_amount, jzpz=jzpz, unusual_id=unusual_id).replace('\r','').replace('\n','').strip()


        if idx < batch_size :
            tmp_ls.append(value_sql)
        elif idx == batch_size :
            # 满一个批次进行处理操作
            tmp_ls.append(value_sql)
            result_sql = insert_sql + ",".join(tmp_ls)
            sqllist.append(result_sql)
            #print('* inner batch tmp_ls=', ','.join(tmp_ls))
            tmp_ls.clear()
            idx = 0

    if tmp_ls:
        # print('* outer batch tmp_ls=', ','.join(tmp_ls))
        result_sql = insert_sql + ",".join(tmp_ls)
        sqllist.append(result_sql)

    log.info('* ready sql for insert')
    for insert_sql in sqllist:
        #print(insert_sql)
        prod_execute_sql(sqltype='insert', sql=insert_sql)

    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* 插入耗时 {consumed_time} sec')


if __name__ == "__main__":
    check_03_consistent_amount_222()
