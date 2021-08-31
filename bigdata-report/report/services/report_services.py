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


def getKUDUdata(sql):
    records = prod_execute_sql(sqltype='select', sql=sql)
    print(len(records))

    # dataFromKUDU1 = []
    dataFromKUDU2 = []
    for item in records:
        # print(type(item), type(item[0]) , item)
        record = [str(item[0]), str(item[1]), str(item[2])]

        dataFromKUDU2.append(record)
        # dataFromKUDU1.append( item )

    df = pd.DataFrame(data=dataFromKUDU2)
    return df


def query_kudu_data(sql, columns):
    """
    发票日期异常检查
    :return:
    """
    records = prod_execute_sql(sqltype='select', sql=sql)
    log.info('***' * 10 )
    log.info('*** query_kudu_data=>' + str(len(records)))
    log.info('***' * 10 )

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


def check_01_for_abnormal_invoice_date(sql, columns):
    """
    需求1: 发票日期异常检查
    :return:
    """
    records = prod_execute_sql(sqltype='select', sql=sql)
    # print( len(records))

    dataFromKUDU = []
    for item in records:
        record = []
        if columns:
            for idx in range(len(columns)):
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
    sql = 'select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill where check_amount > jzpz limit 1000'.format(
        columns_str=columns_str)
    count_sql = 'select count(a.bill_id) from ({sql}) a'.format(sql=sql)

    records = prod_execute_sql(sqltype='select', sql=count_sql)
    count_records = records[0][0]
    max_size = 1 * 700
    limit_size = 1000
    select_sql_ls = []

    log.info('* count_records={count_records}'.format(count_records=count_records))
    if count_records >= max_size:
        offset_size = 0
        while offset_size <= count_records:

            if offset_size + limit_size > count_records:
                limit_size = count_records - offset_size
                tmp_sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill order by finance_travel_id limit {limit_size} offset {offset_size}".format(
                    columns_str=columns_str, limit_size=limit_size, offset_size=offset_size)

                select_sql_ls.append(tmp_sql)
                break
            else:
                tmp_sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill order by finance_travel_id limit {limit_size} offset {offset_size}".format(
                    columns_str=columns_str, limit_size=limit_size, offset_size=offset_size)
                select_sql_ls.append(tmp_sql)

            offset_size = offset_size + limit_size
    else:
        tmp_sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill".format(
            columns_str=columns_str)
        select_sql_ls.append(tmp_sql)

    #print(len(select_sql_ls), select_sql_ls)
    log.info('* 开始分页查询')

    query_data = []
    for sel_sql in select_sql_ls:
        log.info(sel_sql)
        data = prod_execute_sql(sqltype='select', sql=sel_sql)
        #print(data)
        if data:
            query_data.extend(data)

    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* 查询耗时 {consumed_time} sec')

    # part2 insert data
    #print(len(query_data))
    start_time = time.perf_counter()
    for item in query_data:
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

        insert_sql = """
                            insert into table 01_datamart_layer_007_h_cw_df.finance_all_targets(company_code, bill_id, account_period, 
                                    account_item , finance_number ,cost_center, 
                                    profit_center, cart_head, bill_code, 
                                    origin_city, destin_city, beg_date, end_date,
                                    jour_amount, accomm_amount, 
                                    subsidy_amount, other_amount,
                                    emp_code,emp_name, 
                                    check_amount,  jzpz, unusual_id) 
                                    values( "{company_code}", "{bill_id}" ,  "{account_period}" , 
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
                                               check_amount=check_amount, jzpz=jzpz, unusual_id=unusual_id)

        prod_execute_sql(sqltype='insert', sql=insert_sql)


    consumed_time = round(time.perf_counter() - start_time)
    print(f'* 插入耗时 {consumed_time} sec')


def check_03_consistent_amount():
    """
    需求3： 凭证记账金额与报销单据金额一致检查
    复核检查费用报销原始单据信息与记账凭证金额一致性，检查是否存在费用报销金额大于原始票据金额情况。
    :return:
    """

    # part1: select data
    start_time = time.perf_counter()
    # check_amount费用报销金额， jzpz 票据金额， 检查是否存在费用报销金额大于原始票据金额情况
    columns = ['finance_travel_id', 'bill_id', 'apply_emp_id', 'apply_emp_name', 'check_amount',  'jzpz']
    sql = 'select finance_travel_id ,bill_id, apply_emp_id, apply_emp_name, check_amount,jzpz  from 01_datamart_layer_007_h_cw_df.finance_travel_bill where check_amount > jzpz limit 5 '
    log.info(sql)
    rd_df = query_kudu_data(sql, columns)
    consumed_time = round(time.perf_counter() - start_time)
    print(f'* consumed_time={consumed_time} sec')

    #rd_df['check_amount'] = rd_df['check_amount'].astype('float')
    #rd_df['jzpz'] = rd_df['jzpz'].astype('float')
    #print(rd_df.dtypes)
    print(rd_df.head(5))
    print(rd_df.shape)

    # part2: insert data
    start_time = time.perf_counter()
    for row in rd_df.itertuples():
        sql = """
insert into table 01_datamart_layer_007_h_cw_df.finance_all_targets(finance_travel_id, bill_id, emp_code, emp_name, check_amount, jzpz, unusual_id) values("{finance_travel_id}", "{bill_id}" , 
"{emp_code}", "{emp_name}" , {check_amount} , {jzpz}  , "{unusual_id}" )
        """.format(finance_travel_id=row.finance_travel_id,bill_id=row.bill_id ,emp_code=row.apply_emp_id,emp_name=row.apply_emp_name,
                   check_amount=row.check_amount, jzpz=row.jzpz, unusual_id='03')
        prod_execute_sql(sqltype='insert', sql=sql)

    consumed_time = round(time.perf_counter() - start_time)
    print(f'* insert records consumed_time={consumed_time} sec')

    # part3: upsert data
    # sql ="""
    # upsert into  table 01_datamart_layer_007_h_cw_df.finance_all_targets
    # select
    # finance_travel_id,
    # company_code,
    # account_period ,
    # account_item,
    # finance_number,
    # cost_center,
    # profit_center ,
    # NULL ,
    # bill_id,
    # bill_code,
    # NULL,
    # NULL,
    # bill_beg_date,
    # bill_end_date,
    # apply_emp_name ,
    # NULL,
    # jour_amount ,
    # accomm_amount,
    # subsidy_amount,
    # other_amount,
    # check_amount ,
    # jzpz ,
    # "03"
    # from  01_datamart_layer_007_h_cw_df.finance_travel_bill
    # """
    # prod_execute_sql(sqltype='insert', sql=sql)

    dis_connection()


def check_06_reasonsubsidy_amount():
    """
    需求6： 通过按出差人天和报销标准，重新计算和复核出差补助报销金额，是否复核集团要求和规定，尤其是连续出差超过14天的，是否按照分段报销标准进行计算。
    :return:
    """
    start_time = time.perf_counter()
    # check_amount费用报销金额， jzpz 票据金额， 检查是否存在费用报销金额大于原始票据金额情况
    columns = ['subsidy_bill_id','beg_date', 'end_date', 'check_amount', 'total_date']
    sql = """
   select a.subsidy_bill_id, a.beg_date, a.end_date, a.check_amount,  a.total_date from (
    select subsidy_bill_id, beg_date, end_date ,check_amount, (unix_timestamp(end_date, 'yyyyMMdd')-unix_timestamp(beg_date, 'yyyyMMdd'))/ (60 * 60 * 24) as total_date
    from 01_datamart_layer_007_h_cw_df.finance_rma_travel_subsidy
    where (unix_timestamp(end_date, 'yyyyMMdd')-unix_timestamp(beg_date, 'yyyyMMdd'))/ (60 * 60 * 24)  > 14
    union
    select subsidy_bill_id, beg_date, end_date ,check_amount, (unix_timestamp(end_date, 'yyyyMMdd')-unix_timestamp(beg_date, 'yyyyMMdd'))/ (60 * 60 * 24) as total_date
    from 01_datamart_layer_007_h_cw_df.finance_rma_pictc_allowance
    where (unix_timestamp(end_date, 'yyyyMMdd')-unix_timestamp(beg_date, 'yyyyMMdd'))/ (60 * 60 * 24)  > 14
)a, (select standard_value, out_value from 01_datamart_layer_007_h_cw_df.finance_standard where unusual_id='06') b
where  a.check_amount > ( 14 * b.standard_value + (a.total_date - 14 ) * b.out_value )
    """
    log.info(sql)
    rd_df = query_kudu_data(sql, columns)
    consumed_time = round(time.perf_counter() - start_time)
    print(f'* consumed_time={consumed_time} sec')

    #rd_df['total_date'] = rd_df['total_date'].astype('int')
    print(rd_df.head(5))
    print(rd_df.shape)

    start_time = time.perf_counter()
    for row in rd_df.itertuples():
        sql = 'insert into 01_datamart_layer_007_h_cw_df.finance_06_reasonsubsidy values("{subsidy_bill_id}", "{beg_date}", "{end_date}", {total_date}, {check_amount} ,"{unusual_id}") '.format(
            subsidy_bill_id=row.subsidy_bill_id, beg_date=row.beg_date, end_date=row.end_date,
            total_date=int(float(row.total_date)), check_amount=row.check_amount,
            unusual_id='06'
        )
        #print(sql)
        #prod_execute_sql(sqltype='insert', sql=sql)
        pass

    consumed_time = round(time.perf_counter() - start_time)
    print(f'* insert records consumed_time={consumed_time} sec')

    dis_connection()



def check_10_beforeapply_amount():
    """
    需求10： 报销申请单不存在关联的事前申请单号，或者事前申请的日期晚于出差开始日期（即差旅行程的出发日期）
    :return:
    """

    # 需求1，
    sql = """
    select
   t.bill_apply_id,
   t.apply_id,
   t.apply_beg_date,
   b.beg_date
from 01_datamart_layer_007_h_cw_df.finance_travel_bill t,
(select b.bill_id,  min(b.beg_date) as beg_date from 01_datamart_layer_007_h_cw_df.finance_rma_travel_journey b group by b.bill_id )b
where t.bill_id = b.bill_id and unix_timestamp(t.apply_beg_date, 'yyyyMMdd') > unix_timestamp(b.beg_date, 'yyyyMMdd')
    """
    columns = ['bill_apply_id', 'apply_id', 'apply_beg_date', 'beg_date']
    start_time = time.perf_counter()
    rd_df1 = query_kudu_data(sql, columns)
    consumed_time = round(time.perf_counter() - start_time)
    print(f'* query1 records consumed_time={consumed_time} sec')

    print(rd_df1.shape)
    start_time = time.perf_counter()
    for row in rd_df1.itertuples():
        sql = 'insert into 01_datamart_layer_007_h_cw_df.finance_10_beforeapply values( "{bill_apply_id}", "{apply_id}", "{apply_beg_date}", "{beg_date}" ,"{unusual_id}") '.format(
            bill_apply_id=row.bill_apply_id, apply_id=row.apply_id,
            apply_beg_date=row.apply_beg_date, beg_date=row.beg_date,
            unusual_id='10'
        )
        #prod_execute_sql(sqltype='insert', sql=sql)
        pass

    consumed_time = round(time.perf_counter() - start_time)
    print(f'* insert1 records consumed_time={consumed_time} sec')

    # 需求2： 事前申请的日期晚于出差开始日期（即差旅行程的出发日期）
    sql = """
    select b.bill_apply_id, b.apply_id , b.bill_apply_date, from_unixtime(j.t_beg_date, 'yyyyMMdd')as beg_date  from
   01_datamart_layer_007_h_cw_df.finance_travel_bill b, (
   select bill_id, min(unix_timestamp(beg_date, 'yyyyMMdd')) as t_beg_date  from  01_datamart_layer_007_h_cw_df.finance_rma_travel_journey
group by bill_id
   )j
   where b.bill_id = j.bill_id
   and  unix_timestamp(b.base_apply_date, 'yyyyMMdd')  > j.t_beg_date
    """

    start_time = time.perf_counter()
    columns = ['bill_apply_id', 'apply_id', 'bill_apply_date', 'beg_date']
    rd_df2 = query_kudu_data(sql, columns)
    consumed_time = round(time.perf_counter() - start_time)
    print(f'* query2 records consumed_time={consumed_time} sec')
    print(rd_df2.shape)

    start_time = time.perf_counter()
    for row in rd_df2.itertuples():
        sql = 'insert into 01_datamart_layer_007_h_cw_df.finance_10_beforeapply values( "{bill_apply_id}", "{base_apply_id}", "{apply_date}", "{journey_date}" ,"{unusual_id}") '.format(
            bill_apply_id=row.bill_apply_id, base_apply_id=row.apply_id,
            apply_date=row.bill_apply_date, journey_date=row.beg_date,
            unusual_id='10'
        )
        #prod_execute_sql(sqltype='insert', sql=sql)
        pass

    consumed_time = round(time.perf_counter() - start_time)
    print(f'* insert2 records consumed_time={consumed_time} sec')

    dis_connection()


def check_15_coststructure_data():
    """
    需求15： 复核是否存在报销信息中无行程信息或者住宿信息的情况；报销费用单据中，缺少住宿费或者交通费用类票据。
    :return:
    """
    start_time = time.perf_counter()
    columns = ['finance_travel_id', 'bill_id', 'bill_apply_id', 'accomm_amount', 'jour_amount']
    sql = "select finance_travel_id , bill_id, bill_apply_id, accomm_amount, jour_amount from 01_datamart_layer_007_h_cw_df.finance_travel_bill where accomm_amount=0 or jour_amount=0"
    log.info(sql)
    rd_df = query_kudu_data(sql, columns)
    consumed_time = round(time.perf_counter() - start_time)
    print(f'* query records consumed_time={consumed_time} sec')

    print(rd_df.head(5))
    print(rd_df.shape)

    start_time = time.perf_counter()
    for row in rd_df.itertuples():
        sql = 'insert into 01_datamart_layer_007_h_cw_df.finance_15_coststructure values("{finance_travel_id}", "{base_bill_id}", "{bill_apply_id}", {accom_amount}, {journey_amount} ,"{unusual_id}") '.format(
            finance_travel_id=row.finance_travel_id, base_bill_id=row.bill_id, bill_apply_id=row.bill_apply_id,
            accom_amount=row.accomm_amount, journey_amount=row.jour_amount,
            unusual_id='15'
        )

        # print(sql)
        #prod_execute_sql(sqltype='insert', sql=sql)
        break


    consumed_time = round(time.perf_counter() - start_time)
    print(f'* insert records consumed_time={consumed_time} sec')

    dis_connection()


def check_22_checktimely_data():
    start_time = time.perf_counter()
    columns = ['', '', '', '', '']
    sql = ""
    rd_df = query_kudu_data(sql, columns)
    consumed_time = round(time.perf_counter() - start_time)
    print(f'* query records consumed_time={consumed_time} sec')

    print(rd_df.head(5))
    print(rd_df.shape)



if __name__ == "__main__":
    # columns = ['finance_travel_id', 'bill_id', 'apply_emp_id', 'apply_emp_name', 'check_amount',  'jzpz']
    # sql = 'select finance_travel_id ,bill_id, apply_emp_id, apply_emp_name, check_amount,jzpz  from 01_datamart_layer_007_h_cw_df.finance_travel_bill where check_amount > jzpz'
    # log.info(sql)
    # rd_df = query_kudu_data(sql, columns)
    # dis_connection()
    # print(rd_df.shape)
    # print('-- ok --')

    check_03_consistent_amount_222()

    # 需求3 , 完成, 待验证
    #check_03_consistent_amount()

    # 需求6 ，完成, 待验证
    #check_06_reasonsubsidy_amount()

    # 需求10 ，完成, 待验证
    #check_10_beforeapply_amount()

    # 需求15 ，完成, 待验证
    #check_15_coststructure_data()


