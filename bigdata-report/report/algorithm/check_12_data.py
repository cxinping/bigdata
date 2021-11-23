# -*- coding: utf-8 -*-

from report.commons.logging import get_logger
import os, time
import re
from string import punctuation
from string import digits
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
import threading
from report.services.common_services import ProvinceService
from report.commons.test_hdfs_tools import HDFSTools as Test_HDFSTools
import pandas as pd
from report.commons.connect_kudu import prod_execute_sql
from report.commons.tools import (list_of_groups, kill_pid)
from report.services.common_services import query_billds_finance_all_targets
from report.commons.settings import CONN_TYPE

"""

SELECT bill_id, emp_name, origin_name, destin_name, beg_date, end_date, traf_name, 出差城市
FROM 01_datamart_layer_007_h_cw_df.finance_rma_travel_journey

SELECT distinct bill_id,origin_name, destin_name,travel_city_name,traf_name, travel_beg_date,travel_end_date
FROM 01_datamart_layer_007_h_cw_df.finance_travel_bill
WHERE origin_name is not NULL and origin_name !='' and destin_name is not NULL and destin_name !='' and destin_name !='NULL'  
AND travel_city_name is not NULL AND travel_city_name !=''
AND travel_beg_date != travel_end_date AND travel_beg_date > '20190101'


"""

log = get_logger(__name__)

import sys

sys.path.append('/you_filed_algos/app')

dest_dir = '/you_filed_algos/prod_kudu_data/checkpoint12'
dest_file = dest_dir + '/check_12_data.txt'

conn_type = 'test'
test_limit_cond = ' '  # ' LIMIT 100010 '


class Check12Service:

    def __init__(self):
        pass

    def init_file(self):
        if not os.path.exists(dest_dir):
            os.makedirs(dest_dir)

        if os.path.exists(dest_file):
            os.remove(dest_file)

    def save_data(self):
        self.init_file()

        columns_ls = ['bill_id', 'origin_name', 'destin_name', 'travel_city_name', 'travel_beg_date',
                      'travel_end_date']
        columns_str = ",".join(columns_ls)

        sql = """
        select distinct {columns_str} FROM 01_datamart_layer_007_h_cw_df.finance_travel_bill
        WHERE origin_name is not NULL and origin_name !='' and destin_name is not NULL and destin_name !='' and destin_name !='NULL'  
        AND travel_city_name is not NULL AND travel_city_name !=''
        AND travel_beg_date != travel_end_date AND  travel_beg_date > '20190101'
        {test_limit_cond}
        """.format(columns_str=columns_str, test_limit_cond=test_limit_cond).replace('\r', '').replace('\n',
                                                                                                       '').replace('\t',
                                                                                                                   '')

        count_sql = 'select count(a.bill_id) from ({sql}) a'.format(sql=sql)
        log.info(count_sql)
        records = prod_execute_sql(conn_type='test', sqltype='select', sql=count_sql)
        count_records = records[0][0]
        log.info(f'* count_records ==> {count_records}')

        max_size = 10 * 10000
        limit_size = 2 * 10000
        select_sql_ls = []
        log.info('* 开始分页查询')

        if count_records >= max_size:
            offset_size = 0
            while offset_size <= count_records:
                if offset_size + limit_size > count_records:
                    limit_size = count_records - offset_size
                    tmp_sql = """
                select distinct {columns_str} FROM 01_datamart_layer_007_h_cw_df.finance_travel_bill
                WHERE origin_name is not NULL and origin_name !='' and destin_name is not NULL and destin_name !='' and destin_name !='NULL'  
                AND travel_city_name is not NULL AND travel_city_name !=''
                AND travel_beg_date != travel_end_date AND travel_beg_date > '20190101'                    
                ORDER BY travel_beg_date limit {limit_size} offset {offset_size}
                    """.format(limit_size=limit_size, offset_size=offset_size, columns_str=columns_str).replace('\r',
                                                                                                                '').replace(
                        '\n', '').replace('\t', '')

                    select_sql_ls.append(tmp_sql)
                    break
                else:
                    tmp_sql = """
                    select distinct {columns_str} FROM 01_datamart_layer_007_h_cw_df.finance_travel_bill
                    WHERE origin_name is not NULL and origin_name !='' and destin_name is not NULL and destin_name !='' and destin_name !='NULL'  
                    AND travel_city_name is not NULL AND travel_city_name !=''
                    AND travel_beg_date != travel_end_date AND travel_beg_date > '20190101'                    
                    ORDER BY travel_beg_date limit {limit_size} offset {offset_size}
                        """.format(limit_size=limit_size, offset_size=offset_size,
                                   columns_str=columns_str).replace('\r', '').replace('\n', '').replace('\t', '')

                    select_sql_ls.append(tmp_sql)

                offset_size = offset_size + limit_size
        else:
            tmp_sql = f"""           
        select distinct {columns_str} FROM 01_datamart_layer_007_h_cw_df.finance_travel_bill
        WHERE origin_name is not NULL and origin_name !='' and destin_name is not NULL and destin_name !='' and destin_name !='NULL'  
        AND travel_city_name is not NULL AND travel_city_name !=''
        AND travel_beg_date != travel_end_date AND travel_beg_date > '20190101'
         {test_limit_cond}   
        """.replace('\r', '').replace('\n', '').replace('\t', '')
            select_sql_ls.append(tmp_sql)
            # print('*** tmp_sql => ', tmp_sql)

        log.info(f'*** 开始分页查询，一共 {len(select_sql_ls)} 页')

        threadPool = ThreadPoolExecutor(max_workers=30, thread_name_prefix="thr")
        start_time = time.perf_counter()

        all_task = [threadPool.submit(self.exec_task, (sel_sql)) for sel_sql in select_sql_ls]
        wait(all_task, return_when=ALL_COMPLETED)

        threadPool.shutdown(wait=True)
        consumed_time = round(time.perf_counter() - start_time)
        log.info(f'* 查询耗时 {consumed_time} sec')

    def exec_task(self, sql):
        #log.info(sql)
        records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sql)
        print(0.01)

        if records and len(records) > 0:
            for idx, record in enumerate(records):
                bill_id = str(record[0])  # bill_id
                origin_name = str(record[1])  # 出发地
                destin_name = str(record[2])  # 目的地
                travel_city_name = str(record[3])  # 出差城市
                travel_beg_date = str(record[4])  # 差旅开始时间
                travel_end_date = str(record[5])  # 差旅结束时间

                travel_city_name = re.sub(r'[{}]+'.format(punctuation + digits), ' ', travel_city_name)
                # log.info(travel_city_name)

                record_str = f'{bill_id},{origin_name},{destin_name},{travel_city_name},{travel_beg_date},{travel_end_date}'
                log.info(f"checkpoint_12 {threading.current_thread().name} is running ")
                log.info(record_str)
                print()

                with open(dest_file, "a+", encoding='utf-8') as file:
                    file.write(record_str + "\n")

    def complex_function(self, travel_city_name, origin_name, destin_name):
        if travel_city_name is None or travel_city_name == 'NULL':
            return ''

        # print(travel_city_name)
        travel_city_names = travel_city_name.strip().split(' ')
        #print(f'1*** travel_city_names => {travel_city_names}')
        #print(f'2*** len(travel_city_names) => {len(travel_city_names)}')

        trans_travel_city_name = ''
        if travel_city_names and len(travel_city_names) > 1:
            travel_city_names.sort()
            #print('3*** travel_city_names => ', travel_city_names, type(travel_city_names))

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

        #print('4*** trnas_travel_city_name => ', trnas_travel_city_name)
        #print()

        return trans_travel_city_name

    def cal_df_data(self, group_df, origin_name, destin_name, bill_id_ls):

        group_df['trans_travel_city_name'] = group_df.apply(
            lambda x: self.complex_function(x['travel_city_name'], origin_name, destin_name), axis=1)

        log.info('* before filter')
        print(group_df)
        group_df = group_df[group_df.duplicated('trans_travel_city_name', keep=False) == False]
        log.info('* after filter')
        print(group_df)
        print()

        for index, row in group_df.iterrows():
            bill_id = row['bill_id']
            bill_id_ls.append(bill_id)

    def analyze_data(self):
        log.info('======= check_12 analyze_data_data ===========')

        rd_df = pd.read_csv(dest_file, sep=',', header=None,
                            names=['bill_id', 'origin_name', 'destin_name', 'travel_city_name', 'travel_beg_date',
                                   'travel_end_date'])
        # print(rd_df.head())
        # print(len(rd_df))

        rd_df = rd_df[:700]
        # 测试1
        #rd_df = rd_df[(rd_df['origin_name'] == '宁波市') & (rd_df['destin_name'] == '南京市')]

        grouped_df = rd_df.groupby(['origin_name', 'destin_name'], as_index=False, sort=False)
        bill_id_ls = []
        for name, group_df in grouped_df:
            origin_name, destin_name = name

            if len(group_df) >= 2:
                print(f'*** origin_name={origin_name},destin_name={destin_name}')
                self.cal_df_data(group_df=group_df, origin_name=origin_name, destin_name=destin_name,
                                 bill_id_ls=bill_id_ls)

        print('*** len(rd_df) => ', len(rd_df))
        print('*** len(bill_id_ls) => ', len(bill_id_ls))


if __name__ == "__main__":
    check12_service = Check12Service()
    #check12_service.save_data()  # 查询耗时 1181 sec， 1146783  484799
    check12_service.analyze_data()

    os._exit(0)  # 无错误退出