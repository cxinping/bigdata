# -*- coding: utf-8 -*-
"""
会议费异常检查
@author: WangShuo

01_datamart_layer_007_h_cw_df.finance_meeting_bill
"""

import time

from report.commons.connect_kudu import prod_execute_sql
from report.commons.logging import get_logger
from report.commons.tools import match_address
from report.commons.db_helper import query_kudu_data
from report.services.vehicle_expense_service import cal_commodityname_function
from report.commons.tools import not_empty
from report.commons.db_helper import Pagination, db_fetch_to_dict
import jieba.analyse as analyse
import jieba
from string import punctuation
from string import digits
import re

log = get_logger(__name__)


def check_24_invoice():
    start_time = time.perf_counter()
    sql = """
    UPSERT into  analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets 
    SELECT bill_id, 
    '24' as unusual_id,
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
    met_bgdate  as beg_date,
    met_endate  as end_date,
    '' as emp_name,
    '' as emp_code,
    0 as jour_amount,
    0 as accomm_amount,
    0 as subsidy_amount,
    0 as other_amount,
    check_amount,
    jzpz,
    '会议费',
    met_money
    from 01_datamart_layer_007_h_cw_df.finance_meeting_bill  
    where  billingdate is not null and met_bgdate is not null and met_endate is not null
    and (unix_timestamp(billingdate, 'yyyy-MM-dd HH:mm:ss')< unix_timestamp(met_bgdate,'yyyyMMdd')
    or unix_timestamp(billingdate, 'yyyy-MM-dd HH:mm:ss')> unix_timestamp(met_endate,'yyyyMMdd'))
    """
    prod_execute_sql(sqltype='insert', sql=sql)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* check_24_invoice SQL耗时 {consumed_time} sec')
    # dis_connection()


def check_25_meeting_address():
    columns_ls = ['meet_addr', 'sales_name', 'sales_addressphone', 'sales_bank', 'bill_id']
    columns_str = ",".join(columns_ls)

    sql = """
    select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_meeting_bill where meet_addr is not null and (sales_name is not null and sales_addressphone is not null and sales_bank is not null )  
    """.format(columns_str=columns_str)

    start_time = time.perf_counter()
    select_sql_ls = []
    select_sql_ls.append(sql)
    query_data = []
    for sel_sql in select_sql_ls:
        data = prod_execute_sql(sqltype='select', sql=sel_sql)
        if data:
            query_data.extend(data)

    print('*** 查询记录数量 => ', len(query_data))

    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* 查询耗时 {consumed_time} sec')

    match_bill_id_ls = []
    for data in query_data:
        meet_addr = str(data[0])
        sales_name = str(data[1])
        sales_addressphone = str(data[2])
        sales_bank = str(data[3])
        bill_id = str(data[4])

        # print('111 check ', meet_addr, sales_name, sales_addressphone, sales_bank)

        is_match = False
        if sales_name != 'None' or sales_addressphone != 'None' or sales_bank != 'None':
            # sales_name 只匹配市和县
            if sales_name != 'None':
                sales_name_city = match_address(place=sales_name, key='市')
                if sales_name_city != None:
                    if sales_name_city.find(meet_addr) > -1 or meet_addr.find(sales_name_city) > -1:
                        is_match = True
                        continue

                sales_name_city = match_address(place=sales_name, key='县')
                if sales_name_city != None:
                    if sales_name_city.find(meet_addr) > -1 or meet_addr.find(sales_name_city) > -1:
                        is_match = True
                        continue

            # sales_addressphone 只匹配市和县
            if sales_addressphone != 'None':
                sales_addressphone_city = match_address(place=sales_addressphone, key='市')
                if sales_addressphone_city != None:
                    if sales_addressphone_city.find(meet_addr) > -1 or meet_addr.find(sales_addressphone_city) > -1:
                        is_match = True
                        continue

                sales_addressphone_city = match_address(place=sales_addressphone, key='县')
                if sales_addressphone_city != None:
                    if sales_addressphone_city.find(meet_addr) > -1 or meet_addr.find(sales_addressphone_city) > -1:
                        is_match = True
                        continue

            # sales_bank 只匹配市和县
            if sales_bank != 'None':
                sales_bank_city = match_address(place=sales_bank, key='市')
                if sales_bank_city != None:
                    if sales_bank_city.find(meet_addr) > -1 or meet_addr.find(sales_bank_city) > -1:
                        is_match = True
                        continue

                sales_bank_city = match_address(place=sales_bank, key='县')
                if sales_bank_city != None:
                    if sales_bank_city.find(meet_addr) > -1 or meet_addr.find(sales_bank_city) > -1:
                        is_match = True
                        continue

            if is_match == False:
                match_bill_id_ls.append("'" + bill_id + "'")

    if len(match_bill_id_ls) > 0:
        columns_str = ",".join(match_bill_id_ls)

        sql = f"""
    UPSERT into  analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets 
    SELECT bill_id, 
    '25' as unusual_id,
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
    met_bgdate  as beg_date,
    met_endate  as end_date,
    '' as emp_name,
    '' as emp_code,
    0 as jour_amount,
    0 as accomm_amount,
    0 as subsidy_amount,
    0 as other_amount,
    check_amount,
    jzpz,
    '会议费',
    met_money
    from 01_datamart_layer_007_h_cw_df.finance_meeting_bill where bill_id in ({columns_str})        
        """
    start_time = time.perf_counter()
    prod_execute_sql(sqltype='insert', sql=sql)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* 执行 SQL 耗时 {consumed_time} sec')
    # dis_connection()


def check_27_consistent_amount():
    start_time = time.perf_counter()
    sql = """
    UPSERT into  analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets
    SELECT bill_id, 
    '27' as unusual_id,
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
    met_bgdate  as beg_date,
    met_endate  as end_date,
    '' as emp_name,
    '' as emp_code,
    0 as jour_amount,
    0 as accomm_amount,
    0 as subsidy_amount,
    0 as other_amount,
    check_amount,
    jzpz,
    '会议费',
    met_money
    from 01_datamart_layer_007_h_cw_df.finance_meeting_bill  
    where check_amount > jzpz
        """
    prod_execute_sql(sqltype='insert', sql=sql)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* check_27_consistent_amount SQL耗时 {consumed_time} sec')
    # dis_connection()


def check_28_meeting():
    start_time = time.perf_counter()
    sql = """
    UPSERT into  analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets
    SELECT bill_id, 
    '28' as unusual_id,
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
    met_bgdate  as beg_date,
    met_endate  as end_date,
    '' as emp_name,
    '' as emp_code,
    0 as jour_amount,
    0 as accomm_amount,
    0 as subsidy_amount,
    0 as other_amount,
    check_amount,
    jzpz,
    '会议费',
    met_money
    from 01_datamart_layer_007_h_cw_df.finance_meeting_bill  
    where bill_id in (
    select a.bill_id from 01_datamart_layer_007_h_cw_df.finance_meeting_bill a,(
select concat(province, city,county,address,scenery_name) as scenery_name_details from 01_datamart_layer_007_h_cw_df.finance_scenery
)b
where a.meet_addr is not null and b.scenery_name_details like concat('%', a.meet_addr , '%') 
    )
        """
    prod_execute_sql(sqltype='insert', sql=sql)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* check_28_meeting SQL耗时 {consumed_time} sec')
    # dis_connection()


def check_29_cost():
    start_time = time.perf_counter()
    sql = """
        UPSERT into  analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets 
        SELECT bill_id, 
        '29' as unusual_id,
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
        met_bgdate  as beg_date,
        met_endate  as end_date,
        '' as emp_name,
        '' as emp_code,
        0 as jour_amount,
        0 as accomm_amount,
        0 as subsidy_amount,
        0 as other_amount,
        check_amount,
        jzpz,
        '会议费',
        met_money
        from 01_datamart_layer_007_h_cw_df.finance_meeting_bill  
        where bill_id in (
            select 
                bill_id
                from 01_datamart_layer_007_h_cw_df.finance_meeting_bill 
                where meet_type_name is not null 
                and met_bgdate is not null 
                and met_endate is not null
                and met_money is not null
                and meet_num is not null 
                and (case 
                when meet_type_name='集团外部会议' then 550
                when meet_type_name='党组决议召开会议' then 550
                when meet_type_name='系统外商业会议' then 500
                when meet_type_name='系统内会议' then 550
                when meet_type_name='企业自办会议' then 550
                when meet_type_name='集团内部会议' then 550
                when meet_type_name='集团公司工作会议' then 550
                else 500 end)<cast(met_money as int)/(cast(meet_num as int)*(datediff(from_unixtime(unix_timestamp(met_endate,'yyyyMMdd'),'yyyy-MM-dd'), from_unixtime(unix_timestamp(met_bgdate,'yyyyMMdd'),'yyyy-MM-dd'))+1))
        )
            """
    prod_execute_sql(sqltype='insert', sql=sql)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* check_29_cost SQL耗时 {consumed_time} sec')
    # dis_connection()


def check_30_apply_data():
    start_time = time.perf_counter()
    sql = """
     UPSERT into  analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets
    SELECT bill_id, 
    '30' as unusual_id,
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
    met_bgdate  as beg_date,
    met_endate  as end_date,
    '' as emp_name,
    '' as emp_code,
    0 as jour_amount,
    0 as accomm_amount,
    0 as subsidy_amount,
    0 as other_amount,
    check_amount,
    jzpz,
    '会议费',
    met_money
    from 01_datamart_layer_007_h_cw_df.finance_meeting_bill    
    where apply_id = '' or base_apply_date > met_bgdate 
            """

    prod_execute_sql(sqltype='insert', sql=sql)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* check_30_apply_data SQL耗时 {consumed_time} sec')
    # dis_connection()


def check_33_meeting_level():
    start_time = time.perf_counter()
    sql = """
     UPSERT into  analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets
    SELECT bill_id, 
    '33' as unusual_id,
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
    met_bgdate  as beg_date,
    met_endate  as end_date,
    '' as emp_name,
    '' as emp_code,
    0 as jour_amount,
    0 as accomm_amount,
    0 as subsidy_amount,
    0 as other_amount,
    check_amount,
    jzpz,
    '会议费',
    met_money
    from 01_datamart_layer_007_h_cw_df.finance_meeting_bill    
    where meet_lvl_name in (
      select t.meet_lvl_name from (
        select round(avg(a.diff_met_date)) as  diff_met_date , a.meet_lvl_name ,(case 
        when a.meet_lvl_name='一类会议' then '1'
        when a.meet_lvl_name='二类会议' then '2'
        when a.meet_lvl_name='三类会议' then '3'
        when a.meet_lvl_name='四类会议' then '4'
        else '0' end) as meet_lvl
        from (
        select  bill_id,meet_lvl_name, met_bgdate,met_endate, (unix_timestamp(met_endate, 'yyyyMMdd')-unix_timestamp(met_bgdate, 'yyyyMMdd'))/ (60 * 60 * 24) as diff_met_date 
        from 01_datamart_layer_007_h_cw_df.finance_meeting_bill where (meet_lvl_name is not null and meet_lvl_name !='')
        and met_bgdate is not null and met_bgdate is not null
        ) a group by a.meet_lvl_name )t, 01_datamart_layer_007_h_cw_df.finance_standard s
        where t.meet_lvl = s.unusual_level and s.unusual_id = '39'
        and diff_met_date > s.out_value        
    )
            """

    prod_execute_sql(sqltype='insert', sql=sql)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* check_33_meeting_level SQL耗时 {consumed_time} sec')
    # dis_connection()


def check_38_credit():
    start_time = time.perf_counter()
    sql = """
          UPSERT into  analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets
         SELECT bill_id, 
         '38' as unusual_id,
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
         met_bgdate  as beg_date,
         met_endate  as end_date,
         '' as emp_name,
         '' as emp_code,
         0 as jour_amount,
         0 as accomm_amount,
         0 as subsidy_amount,
         0 as other_amount,
         check_amount,
         jzpz,
         '会议费',
         met_money
         from 01_datamart_layer_007_h_cw_df.finance_meeting_bill    
         where account_period is not null 
            and arrivedtimes is not null
            and cast(substr(account_period,5,3) as int)> cast(substr(arrivedtimes,6,2)  as int)
        """

    # print(sql)

    prod_execute_sql(sqltype='insert', sql=sql)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* check_38_credit SQL耗时 {consumed_time} sec')
    # dis_connection()


def check_37_ticket():
    start_time = time.perf_counter()
    sql = """
      UPSERT into  analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets
     SELECT bill_id, 
     '37' as unusual_id,
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
     met_bgdate  as beg_date,
     met_endate  as end_date,
     '' as emp_name,
     '' as emp_code,
     0 as jour_amount,
     0 as accomm_amount,
     0 as subsidy_amount,
     0 as other_amount,
     check_amount,
     jzpz,
     '会议费',
     met_money
     from 01_datamart_layer_007_h_cw_df.finance_meeting_bill    
     where bill_id in (     
           
     )
      """

    # print(sql)

    prod_execute_sql(sqltype='insert', sql=sql)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* check_37_ticket SQL耗时 {consumed_time} sec')
    # dis_connection()


def check_39_reimburse():
    start_time = time.perf_counter()
    sql = """
      UPSERT into  analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets
     SELECT bill_id, 
     '39' as unusual_id,
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
     met_bgdate  as beg_date,
     met_endate  as end_date,
     '' as emp_name,
     '' as emp_code,
     0 as jour_amount,
     0 as accomm_amount,
     0 as subsidy_amount,
     0 as other_amount,
     check_amount,
     jzpz,
     '会议费',
     met_money
     from 01_datamart_layer_007_h_cw_df.finance_meeting_bill    
     where bill_id in (     
            select a.bill_id  from
                (select bill_id, (unix_timestamp(bill_apply_date, 'yyyyMMdd')-unix_timestamp(met_endate, 'yyyyMMdd'))/ (60 * 60 * 24) as diff_date 
                from  01_datamart_layer_007_h_cw_df.finance_meeting_bill where bill_apply_date > met_endate)a, (
                select standard_value from 01_datamart_layer_007_h_cw_df.finance_standard where unusual_id='39')b
                where a.diff_date > b.standard_value  
     )
      """

    # print(sql)

    prod_execute_sql(sqltype='insert', sql=sql)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* check_39_reimburse SQL耗时 {consumed_time} sec')
    # dis_connection()


def query_checkpoint_26_commoditynames():
    columns_ls = ['commodityname']
    columns_str = ",".join(columns_ls)

    sql = f'select distinct {columns_str} from 01_datamart_layer_007_h_cw_df.finance_meeting_bill where commodityname is not null and commodityname != "" '
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


def get_conference_bill_jiebaword():
    """
    抽取 会议费的关键字
    :return:
    """

    commodityname_ls = query_checkpoint_26_commoditynames()
    sql = "select distinct commodityname from 01_datamart_layer_007_h_cw_df.finance_meeting_bill where commodityname is not null and commodityname !=''"
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
    final_list = analyse.extract_tags(words4, topK=50, withWeight=False, allowPOS=())

    return final_list


def pagination_conference_records(categorys, good_keywords):
    """
    分页查询会议费的记录
    :param categorys: 商品分类列表
    :param good_keywords: 商品关键字列表
    :return:
    """
    columns_ls = ['bill_id', 'commodityname', 'bill_type_name']
    columns_str = ",".join(columns_ls)
    sql = f"SELECT {columns_str} FROM 01_datamart_layer_007_h_cw_df.finance_meeting_bill "

    count_sql = 'SELECT count(a.bill_id) FROM ({sql}) a'.format(sql=sql)
    log.info(count_sql)
    records = prod_execute_sql(conn_type='test', sqltype='select', sql=count_sql)
    count_records = records[0][0]
    # print('* count_records => ', count_records)

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

    order_sql = ' ORDER BY bill_id ASC '
    sql = sql + where_sql + order_sql

    return count_records, sql, columns_ls


def main():
    # 需求 24 done
    check_24_invoice()

    # 需求25 done
    # check_25_meeting_address()

    # 需求 27 done
    # check_27_consistent_amount()

    # 需求 28 done 未测试 ......
    # check_28_meeting()

    # 需求 29 done 未测试
    # check_29_cost()

    # 需求30 done
    # check_30_apply_data()

    # 需求 33 done 未测试
    # check_33_meeting_level()

    # 需求 38 done 未测试
    # check_38_credit()

    # 需求39 done
    # check_39_reimburse()

    pass


if __name__ == "__main__":
    pass

    # main()

    # records = query_checkpoint_26_commoditynames()
    # print(records)
    #
    # final_list = get_conference_bill_jiebaword()
    # for word in final_list:
    #     print(word)

    categorys = ['运输服务', '包装饮用水', ]  # ['运输服务', '包装饮用水', '住宿服务', '计算机配套产品' ]
    good_keywords = ['手册']  # ['手册', '会议费', '荣誉证书']
    count_records, sql, columns_ls = pagination_conference_records(categorys, good_keywords)
    print(count_records, sql, columns_ls)

    current_page = 1
    page_obj = Pagination(current_page=current_page, all_count=count_records, per_page_num=10)
    records = page_obj.exec_sql(sql, columns_ls)
    print(records)
