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


log = get_logger(__name__)

import sys

sys.path.append('/you_filed_algos/app')

dest_dir = '/you_filed_algos/prod_kudu_data/checkpoint13'
dest_file = dest_dir + '/check_13_data.txt'


def check_13_data():
    """
    关注相同或临近期间，同一费用发生地，不同人员或企业的住宿费偏离平均值或者大多数人费用分布区间的情况。

    住宿总金额 accomm_amount

    :return:
    """
    columns_ls = ['bill_id', 'destin_name', 'accomm_amount']
    columns_str = ",".join(columns_ls)
    sql = 'select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill where destin_name is not null'.format(
        columns_str=columns_str)

    count_sql = """
    select count(*) from (
        select distinct
        a.bill_id, a.city_name, a.city_grade_name, a.emp_name,
        a.hotel_amount/a.hotel_num as hotel_amount_day,
        b.member_level_id, b.member_level_name,
        b.account_period_y
        from 01_datamart_layer_007_h_cw_df.finance_rma_travel_accomm a,
        (
        select distinct
        bill_id,member_level_id,member_level_name, substr(cast(account_period as string),1,4) as account_period_y
        from 01_datamart_layer_007_h_cw_df.finance_travel_bill
        where substr(cast(account_period as string),1,4)='2021' )  b
        where a.exp_type_name="差旅费" and a.hotel_num > 0 and
              a.bill_id =b.bill_id
              ) zzz    
    """
    print(count_sql)
    records = prod_execute_sql(conn_type='test', sqltype='select', sql=count_sql)
    count_records = records[0][0]
    print(f'* count_records ==> {count_records}')


def init_file():
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)

    if os.path.exists(dest_file):
        os.remove(dest_file)


def save_data():
    init_file()

    columns_ls = ['bill_id', 'city_name', 'city_grade_name', 'emp_name', 'hotel_amount/hotel_num']
    columns_str = ",".join(columns_ls)
    sql = 'select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_rma_travel_accomm where exp_type_name="差旅费" and hotel_num > 0 '.format(
        columns_str=columns_str)

    count_sql = 'select count(a.bill_id) from ({sql}) a'.format(sql=sql)
    log.info(count_sql)
    records = prod_execute_sql(conn_type='test', sqltype='select', sql=count_sql)
    count_records = records[0][0]
    print(f'* count_records ==> {count_records}')

    max_size = 10 * 10000
    limit_size = 1000
    select_sql_ls = []
    if count_records >= max_size:
        offset_size = 0
        while offset_size <= count_records:
            if offset_size + limit_size > count_records:
                limit_size = count_records - offset_size
                tmp_sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_rma_travel_accomm where exp_type_name='差旅费' and hotel_num > 0 order by bill_id limit {limit_size} offset {offset_size}".format(
                    columns_str=columns_str, limit_size=limit_size, offset_size=offset_size)

                select_sql_ls.append(tmp_sql)
                break
            else:
                tmp_sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_rma_travel_accomm where exp_type_name='差旅费' and hotel_num > 0 order by bill_id limit {limit_size} offset {offset_size}".format(
                    columns_str=columns_str, limit_size=limit_size, offset_size=offset_size)
                select_sql_ls.append(tmp_sql)

            offset_size = offset_size + limit_size
    else:
        tmp_sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_rma_travel_accomm where exp_type_name='差旅费' and hotel_num > 0 ".format(
            columns_str=columns_str)
        select_sql_ls.append(tmp_sql)
        # print('*** tmp_sql => ', tmp_sql)

    log.info(f'*** 开始分页查询，一共 {len(select_sql_ls)} 页')
    threadPool = ThreadPoolExecutor(max_workers=30)
    start_time = time.perf_counter()

    # for sel_sql in select_sql_ls:
    #     threadPool.submit(exec_task, sel_sql)

    all_task = [threadPool.submit(exec_task, (sel_sql)) for sel_sql in select_sql_ls]
    wait(all_task, return_when=ALL_COMPLETED)

    threadPool.shutdown(wait=True)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* 查询耗时 {consumed_time} sec')


def exec_task(sql):
    records = prod_execute_sql(conn_type='test', sqltype='select', sql=sql)
    if records and len(records) > 0:
        for idx, record in enumerate(records):
            bill_id = str(record[0])
            city_name = str(record[1])
            city_grade_name = str(record[2])
            emp_name = str(record[3])
            hotel_fee = float(record[4])

            record = f'{bill_id},{city_name},{city_grade_name},{emp_name},{hotel_fee}'
            print(record)

            with open(dest_file, "a+", encoding='utf-8') as file:
                file.write(record + "\n")


def load_data():
    rd_df = pd.read_csv(dest_file, sep=',', header=None)
    print(rd_df.dtypes)
    print(len(rd_df))


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

    def query_abnormal_fee(self):
        """
        查询超过标准报销费用的住宿费记录
        :return:
        """
        init_file()

        sql = """
        SELECT a.bill_id, a.city_name, a.city_grade_name, a.emp_name,a.stand_amount_perday, a.hotel_amount_perday
            FROM (
            SELECT DISTINCT
            bill_id, city_name, city_grade_name, emp_name,ROUND(stand_amount, 2) as stand_amount_perday,
            ROUND(hotel_amount/hotel_num , 2) as hotel_amount_perday
            FROM 01_datamart_layer_007_h_cw_df.finance_rma_travel_accomm
            WHERE exp_type_name="差旅费" AND hotel_num > 0
            )as a
        WHERE a.hotel_amount_perday  > a.stand_amount_perday
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
                        WHERE a.hotel_amount_perday  > a.stand_amount_perday
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
                           WHERE a.hotel_amount_perday  > a.stand_amount_perday
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
            WHERE exp_type_name="差旅费" AND hotel_num > 0
            )as a
        WHERE a.hotel_amount_perday  > a.stand_amount_perday
        """
            select_sql_ls.append(tmp_sql)
            # print('*** tmp_sql => ', tmp_sql)

        log.info(f'*** 开始分页查询，一共 {len(select_sql_ls)} 页')
        threadPool = ThreadPoolExecutor(max_workers=30)
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

                record_str = f'{bill_id},{city_name},{province},{city_grade_name},{emp_name},{stand_amount_perday},{hotel_amount_perday}'
                log.info(f" {threading.current_thread().name} is doing ")
                log.info(record_str)
                print()

                with open(dest_file, "a+", encoding='utf-8') as file:
                    file.write(record_str + "\n")


if __name__ == "__main__":
    # check_13_data()

    # save_data()
    # load_data()

    check13_service = Check13Service()
    # check13_service.query_areas()
    check13_service.query_abnormal_fee()   # 292812

    print('--- ok ---')
