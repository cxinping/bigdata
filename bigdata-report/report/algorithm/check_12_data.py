# -*- coding: utf-8 -*-

from report.commons.logging import get_logger
import os, time
import re
from string import punctuation
from string import digits
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
import threading
import pandas as pd
from report.commons.connect_kudu2 import prod_execute_sql
from report.commons.tools import list_of_groups
from report.services.common_services import query_bill_codes_finance_all_targets
from report.commons.settings import CONN_TYPE

"""

SELECT bill_id, emp_name, origin_name, destin_name, beg_date, end_date, traf_name, 出差城市
FROM 01_datamart_layer_007_h_cw_df.finance_rma_travel_journey

SELECT distinct bill_id,origin_name, destin_name,travel_city_name,traf_name, travel_beg_date,travel_end_date
FROM 01_datamart_layer_007_h_cw_df.finance_travel_bill
WHERE origin_name is not NULL and origin_name !='' and destin_name is not NULL and destin_name !='' and destin_name !='NULL'  
AND travel_city_name is not NULL AND travel_city_name !=''
AND travel_beg_date != travel_end_date AND travel_beg_date > '20190101'

cd /you_filed_algos/app

PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/algorithm/check_12_data.py 

"""

log = get_logger(__name__)

import sys

sys.path.append('/you_filed_algos/app')

# 设置显示最大列数 与 显示宽度
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 500)

dest_dir = '/you_filed_algos/prod_kudu_data/checkpoint12'
dest_file = dest_dir + '/check_12_data.txt'

test_limit_cond = ' '  # ' LIMIT 100010 '


class Check12Service:

    def __init__(self):
        pass

    def init_file(self):
        if not os.path.exists(dest_dir):
            os.makedirs(dest_dir)

        if os.path.exists(dest_file):
            os.remove(dest_file)

        os.mknod(dest_file)

    def save_data(self):
        self.init_file()

        columns_ls = ['bill_code', 'origin_name', 'destin_name', 'travel_city_name', 'travel_beg_date',
                      'travel_end_date']
        columns_str = ",".join(columns_ls)

        sql = """
        select distinct {columns_str} FROM 01_datamart_layer_007_h_cw_df.finance_travel_bill
        WHERE origin_name is not NULL and origin_name !='' and destin_name is not NULL and destin_name !=''  
        AND travel_city_name is not NULL AND travel_city_name !=''
        AND travel_beg_date != travel_end_date AND travel_beg_date > '20190101' AND bill_code is not NULL AND bill_code !='' 
        {test_limit_cond}
        """.format(columns_str=columns_str, test_limit_cond=test_limit_cond).replace('\r', '').replace('\n',
                                                                                                       '').replace('\t',
                                                                                                                   '')

        count_sql = 'select count(a.bill_code) from ({sql}) a'.format(sql=sql)
        log.info(count_sql)
        records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=count_sql)
        count_records = records[0][0]
        log.info(f'* count_records ==> {count_records}')

        max_size = 10 * 10000
        limit_size = 1 * 10000
        select_sql_ls = []
        log.info('* 开始分页查询')

        if count_records >= max_size:
            offset_size = 0
            while offset_size <= count_records:
                if offset_size + limit_size > count_records:
                    limit_size = count_records - offset_size
                    tmp_sql = """
                select distinct {columns_str} FROM 01_datamart_layer_007_h_cw_df.finance_travel_bill
                WHERE origin_name is not NULL and origin_name !='' and destin_name is not NULL and destin_name !=''  
                AND travel_city_name is not NULL AND travel_city_name !=''
                AND travel_beg_date != travel_end_date AND travel_beg_date > '20190101' AND bill_code is not NULL AND bill_code !=''  
                ORDER BY travel_beg_date limit {limit_size} offset {offset_size}
                    """.format(limit_size=limit_size, offset_size=offset_size, columns_str=columns_str).replace('\r',
                                                                                                                '').replace(
                        '\n', '').replace('\t', '')

                    select_sql_ls.append(tmp_sql)
                    break
                else:
                    tmp_sql = """
                    select distinct {columns_str} FROM 01_datamart_layer_007_h_cw_df.finance_travel_bill
                    WHERE origin_name is not NULL and origin_name !='' and destin_name is not NULL and destin_name !=''  
                    AND travel_city_name is not NULL AND travel_city_name !=''
                    AND travel_beg_date != travel_end_date AND travel_beg_date > '20190101' AND bill_code is not NULL AND bill_code !=''  
                    ORDER BY travel_beg_date limit {limit_size} offset {offset_size}
                        """.format(limit_size=limit_size, offset_size=offset_size,
                                   columns_str=columns_str).replace('\r', '').replace('\n', '').replace('\t', '')

                    select_sql_ls.append(tmp_sql)

                offset_size = offset_size + limit_size
        else:
            tmp_sql = f"""           
        select distinct {columns_str} FROM 01_datamart_layer_007_h_cw_df.finance_travel_bill
        WHERE origin_name is not NULL and origin_name !='' and destin_name is not NULL and destin_name !=''  
        AND travel_city_name is not NULL AND travel_city_name !=''
        AND travel_beg_date != travel_end_date AND travel_beg_date > '20190101' AND bill_code is not NULL
         {test_limit_cond}   
        """.replace('\r', '').replace('\n', '').replace('\t', '')
            select_sql_ls.append(tmp_sql)
            # print('*** tmp_sql => ', tmp_sql)

        log.info(f'*** 开始分页查询，一共 {len(select_sql_ls)} 页')

        threadPool = ThreadPoolExecutor(max_workers=10, thread_name_prefix="thr")
        start_time = time.perf_counter()

        all_task = [threadPool.submit(self.exec_task, (sel_sql)) for sel_sql in select_sql_ls]
        wait(all_task, return_when=ALL_COMPLETED)

        threadPool.shutdown(wait=True)
        consumed_time = round(time.perf_counter() - start_time)
        log.info(f'* 一共有 {count_records} 条数据, 保存数据共耗时 {consumed_time} sec')

    def exec_task(self, sql):
        # log.info(sql)
        records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sql)

        if records and len(records) > 0:
            for idx, record in enumerate(records):
                bill_code = str(record[0])  # bill_code
                origin_name = str(record[1])  # 出发地
                destin_name = str(record[2])  # 目的地
                travel_city_name = str(record[3])  # 出差城市
                travel_beg_date = str(record[4])  # 差旅开始时间
                travel_end_date = str(record[5])  # 差旅结束时间

                travel_city_name = re.sub(r'[{}]+'.format(punctuation + digits), ' ', travel_city_name)
                # log.info(travel_city_name)

                record_str = f'{bill_code},{origin_name},{destin_name},{travel_city_name},{travel_beg_date},{travel_end_date}'
                #log.info(f"checkpoint_12 {threading.current_thread().name} is running ")
                # log.info(record_str)
                # print()

                with open(dest_file, "a+", encoding='utf-8') as file:
                    file.write(record_str + "\n")

    def complex_function(self, travel_city_name, origin_name, destin_name):
        if travel_city_name is None or travel_city_name == 'NULL' or travel_city_name == 'None':
            return ''

        # print(travel_city_name)
        travel_city_names = travel_city_name.strip().split(' ')
        # print(f'1*** travel_city_names => {travel_city_names}')
        # print(f'2*** len(travel_city_names) => {len(travel_city_names)}')

        trans_travel_city_name = ''
        if travel_city_names and len(travel_city_names) > 1:
            travel_city_names.sort()
            # print('3*** travel_city_names => ', travel_city_names, type(travel_city_names))

            if origin_name in travel_city_names and destin_name in travel_city_names:
                if origin_name == destin_name:
                    travel_city_names.remove(origin_name)
                else:
                    travel_city_names.remove(origin_name)
                    travel_city_names.remove(destin_name)
            elif origin_name in travel_city_names:
                travel_city_names.remove(origin_name)
            elif destin_name in travel_city_names:
                travel_city_names.remove(destin_name)

        trans_travel_city_name = " ".join(travel_city_names)

        # print('4*** trnas_travel_city_name => ', trnas_travel_city_name)
        # print()

        return trans_travel_city_name

    def cal_df_data(self, group_df, origin_name, destin_name, bill_code_ls):

        group_df['trans_travel_city_name'] = group_df.apply(
            lambda x: self.complex_function(x['travel_city_name'], origin_name, destin_name), axis=1)

        # log.info('* before filter')
        # print(group_df.head())
        group_df = group_df[group_df.duplicated('trans_travel_city_name', keep=False) == False]
        # log.info('* after filter')
        # print(group_df.head())
        # print()

        for index, row in group_df.iterrows():
            bill_code = row['bill_code']
            bill_code_ls.append(bill_code)

    def analyze_data(self):
        log.info('======= check_12 analyze_data_data ===========')
        start_time = time.perf_counter()

        rd_df = pd.read_csv(dest_file, sep=',', header=None,
                            names=['bill_code', 'origin_name', 'destin_name', 'travel_city_name', 'travel_beg_date',
                                   'travel_end_date'])
        # print(rd_df.head())
        # print(len(rd_df))
        # test
        # rd_df = rd_df[:300]
        # 测试1
        # rd_df = rd_df[(rd_df['origin_name'] == '宁波市') & (rd_df['destin_name'] == '南京市')]

        grouped_df = rd_df.groupby(['origin_name', 'destin_name'], as_index=False, sort=False)
        bill_code_ls = []
        for name, group_df in grouped_df:
            origin_name, destin_name = name

            if len(group_df) >= 2:
                # print(f'*** origin_name={origin_name},destin_name={destin_name}')
                # print(group_df)
                # print()

                self.cal_df_data(group_df=group_df, origin_name=origin_name, destin_name=destin_name,
                                 bill_code_ls=bill_code_ls)

        print('*** len(rd_df) => ', len(rd_df))
        print('*** len(bill_id_ls) => ', len(bill_code_ls))
        targes_finance_bill_codes_ls = query_bill_codes_finance_all_targets(unusual_id='12')
        bill_code_ls = [x for x in bill_code_ls if x not in targes_finance_bill_codes_ls]
        print(f'* after filter len(bill_code_ls)={len(bill_code_ls)}')

        self.exec_sql(bill_code_ls)

        consumed_time = round(time.perf_counter() - start_time)
        log.info(f'* 执行检查点12的数据共耗时 {consumed_time} sec')

    def exec_sql(self, bill_code_ls):
        print('checkpoint_12 exec_sql ==> ', len(bill_code_ls))

        if bill_code_ls and len(bill_code_ls) > 0:
            group_ls = list_of_groups(bill_code_ls, 1000)
            # print(len(group_ls), group_ls)

            condition_sql = ''
            in_codition = 'bill_code IN {temp}'

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
            UPSERT INTO analytic_layer_zbyy_cwyy_014_cwzbbg.finance_all_targets    
                SELECT
                finance_travel_id as finance_id,
                bill_id,
                '12' as unusual_id,
                company_code,
                account_period,
                finance_number,
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
                '' as company_name,
                jour_amount,
                accomm_amount,
                subsidy_amount,
                other_amount,
                check_amount,
                jzpz,
                '差旅费' as target_classify,
                0 as meeting_amount,
                '' as exp_type_name,
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
                origin_province,
                destin_province,
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
                0 as amounttax,
                '' as offset_ratio,
                '' as amounttax_ratio,
                '' as ratio,
                importdate
                FROM 01_datamart_layer_007_h_cw_df.finance_travel_bill
            WHERE {condition_sql}
                """.format(condition_sql=condition_sql)  # .replace('\n', '').replace('\r', '').strip()

            # print(sql)

            try:
                start_time = time.perf_counter()
                prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)
                consumed_time = round(time.perf_counter() - start_time)
                print(f'*** 执行SQL耗时 {consumed_time} sec')
            except Exception as e:
                print(e)
                raise RuntimeError(e)


check12_service = Check12Service()
#check12_service.save_data() # 一共有 2342893 条数据, 保存数据共耗时 1078 sec
check12_service.analyze_data()  #
print('--- ok, check_12 has been completed ---')
