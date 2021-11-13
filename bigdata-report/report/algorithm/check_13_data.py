# -*- coding: utf-8 -*-

from report.commons.connect_kudu import prod_execute_sql
from report.commons.logging import get_logger
from report.commons.db_helper import query_kudu_data
import time
import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
import threading
from report.services.common_services import ProvinceService
from report.commons.test_hdfs_tools import HDFSTools as Test_HDFSTools

log = get_logger(__name__)

"""
异常值：一组测定值中与平均值的偏差超过两倍标准差的测定值

"""

import sys

sys.path.append('/you_filed_algos/app')

dest_dir = '/you_filed_algos/prod_kudu_data/checkpoint13'
dest_file = dest_dir + '/check_13_data.txt'

upload_hdfs_path = 'hdfs:///user/hive/warehouse/02_logical_layer_007_h_lf_cw.db/finance_travel_linshi_analysis/check_13_data.txt'


class Check13Service():

    def __init__(self):
        self.province_service = ProvinceService()

    def query_areas(self):
        columns_ls = ['city_name', 'city_grade_name']
        columns_str = ",".join(columns_ls)
        sql = """
        SELECT  {columns_str}
            FROM (
            SELECT DISTINCT
             city_name, city_grade_name
             FROM 01_datamart_layer_007_h_cw_df.finance_rma_travel_accomm
            WHERE exp_type_name="差旅费" AND hotel_num > 0
            )as a
            ORDER BY a.city_grade_name ASC
        """.format(
            columns_str=columns_str)

        records = prod_execute_sql(conn_type='test', sqltype='select', sql=sql)

        log.info(len(records))

        for record in records:
            print(record)

    def init_file(self):
        if not os.path.exists(dest_dir):
            os.makedirs(dest_dir)

        if os.path.exists(dest_file):
            os.remove(dest_file)

    def save_fee_data(self):
        """
        查询超过标准报销费用的住宿费记录
        :return:
        """
        self.init_file()

        sql = """
        SELECT a.bill_id, a.city_name, a.city_grade_name, a.emp_name,a.stand_amount_perday, a.hotel_amount_perday
            FROM (
            SELECT DISTINCT
            bill_id, city_name, city_grade_name, emp_name,ROUND(stand_amount, 2) as stand_amount_perday,
            ROUND(hotel_amount/hotel_num , 2) as hotel_amount_perday
            FROM 01_datamart_layer_007_h_cw_df.finance_rma_travel_accomm
            WHERE exp_type_name="差旅费" AND hotel_num > 0 
            )as a        
        """

        count_sql = 'select count(b.bill_id) from ({sql}) b'.format(sql=sql)
        log.info(count_sql)
        records = prod_execute_sql(conn_type='test', sqltype='select', sql=count_sql)
        count_records = records[0][0]
        print(f'* count_records ==> {count_records}')

        max_size = 10 * 10000
        limit_size = 10000
        select_sql_ls = []
        if count_records >= max_size:
            offset_size = 0
            while offset_size <= count_records:
                if offset_size + limit_size > count_records:
                    limit_size = count_records - offset_size
                    tmp_sql = """
                        SELECT a.bill_id, a.city_name, a.city_grade_name, a.emp_name,a.stand_amount_perday, a.hotel_amount_perday
                            FROM (
                            SELECT DISTINCT
                            bill_id, city_name, city_grade_name, emp_name,ROUND(stand_amount, 2) as stand_amount_perday,
                            ROUND(hotel_amount/hotel_num , 2) as hotel_amount_perday
                            FROM 01_datamart_layer_007_h_cw_df.finance_rma_travel_accomm
                            WHERE exp_type_name="差旅费" AND hotel_num > 0
                            )as a                       
                        order by bill_id limit {limit_size} offset {offset_size}
                    """.format(limit_size=limit_size, offset_size=offset_size)

                    select_sql_ls.append(tmp_sql)
                    break
                else:
                    tmp_sql = """
                           SELECT a.bill_id, a.city_name, a.city_grade_name, a.emp_name,a.stand_amount_perday, a.hotel_amount_perday
                               FROM (
                               SELECT DISTINCT
                               bill_id, city_name, city_grade_name, emp_name,ROUND(stand_amount, 2) as stand_amount_perday,
                               ROUND(hotel_amount/hotel_num , 2) as hotel_amount_perday
                               FROM 01_datamart_layer_007_h_cw_df.finance_rma_travel_accomm
                               WHERE exp_type_name="差旅费" AND hotel_num > 0
                               )as a                           
                           order by bill_id limit {limit_size} offset {offset_size}
                           """.format(limit_size=limit_size, offset_size=offset_size)

                    select_sql_ls.append(tmp_sql)

                offset_size = offset_size + limit_size
        else:
            tmp_sql = """           
        SELECT a.bill_id, a.city_name, a.city_grade_name, a.emp_name,a.stand_amount_perday, a.hotel_amount_perday
            FROM (
            SELECT DISTINCT
            bill_id, city_name, city_grade_name, emp_name,ROUND(stand_amount, 2) as stand_amount_perday,
            ROUND(hotel_amount/hotel_num , 2) as hotel_amount_perday
            FROM 01_datamart_layer_007_h_cw_df.finance_rma_travel_accomm            
            )as a
        WHERE a.hotel_amount_perday  > a.stand_amount_perday
        """
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
        records = prod_execute_sql(conn_type='test', sqltype='select', sql=sql)
        if records and len(records) > 0:
            for idx, record in enumerate(records):
                bill_id = str(record[0])                # bill_id
                city_name = str(record[1])              # 出差城市名称
                city_grade_name = str(record[2])        # 出差城市等级
                emp_name = str(record[3])               # 员工名字
                stand_amount_perday = float(record[4])  # 每天标准住宿费用
                hotel_amount_perday = float(record[5])  # 每天实际花费的住宿费用

                province = self.province_service.query_belong_province(city_name)  # 出差城市所属的省
                province = province if province else '******'

                city_name = city_name.replace(',', ' ')
                emp_name = emp_name.replace(',', ' ')

                record_str = f'{bill_id},{city_name},{province},{city_grade_name},{emp_name},{stand_amount_perday},{hotel_amount_perday}'
                log.info(f" {threading.current_thread().name} is runing ")
                log.info(record_str)
                print()

                with open(dest_file, "a+", encoding='utf-8') as file:
                    file.write(record_str + "\n")

    def analyze_data(self, coefficient=2):
        rd_df = pd.read_csv(dest_file, sep=',', header=None,
                            names=['bill_id', 'city_name', 'province', 'city_grade_name', 'emp_name',
                                   'stand_amount_perday', 'hotel_amount_perday'])
        # print(rd_df.dtypes)
        print('before filter ', len(rd_df))

        rd_df['consume_amount_perday'] = rd_df['stand_amount_perday'] - rd_df['hotel_amount_perday']

        print(rd_df.head(10))

        # 过滤查询
        query_province = '江苏省'
        rd_df = rd_df[(rd_df['province'] == query_province)
                      & (rd_df['stand_amount_perday'] >= rd_df['hotel_amount_perday'])
                      & (rd_df['consume_amount_perday'] > 0)
                      ]
        print('after filter ', len(rd_df))
        print(rd_df.head(10))

        temp = rd_df.describe()[['consume_amount_perday']]
        std_val = temp.at['std', 'consume_amount_perday']  # 标准差
        mean_val = temp.at['mean', 'consume_amount_perday']  # 平均值

        # 数据的正常范围为 【mean - 2 × std , mean + 2 × std】
        max_val = mean_val + coefficient * std_val
        min_val = mean_val - coefficient * std_val

        print(f'{query_province} 方差 => {std_val}, 平均值 => {mean_val}, max_val => {max_val}, min_val => {min_val}')


if __name__ == "__main__":
    check13_service = Check13Service()
    # check13_service.save_fee_data()   # 一共有数据 5776561 条， 花费时间  秒，
    check13_service.analyze_data(coefficient=2)

    # test_hdfs = Test_HDFSTools(conn_type='test')
    # test_hdfs.uploadFile2(hdfsDirPath=upload_hdfs_path, localPath=dest_file)

    print('--- ok ---')
