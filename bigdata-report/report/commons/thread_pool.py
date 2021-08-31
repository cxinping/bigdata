# -*- coding: utf-8 -*-

from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor  # 线程池，进程池
import threading, time
from report.commons.connect_kudu import prod_execute_sql,  dis_connection
from report.commons.logging import get_logger
import sys
import time
from datetime import datetime


log = get_logger(__name__)



class CheckWork03(object):

    def __init__(self):
        pass

    def exec_select_data(self):
        columns_ls = ['company_code', 'bill_id', 'account_period', 'account_item', 'finance_number', 'cost_center',
                      'profit_center', 'bill_code', 'origin_name', 'destin_name', 'travel_beg_date', 'travel_end_date',
                      'jour_amount', 'accomm_amount', 'subsidy_amount', 'other_amount',
                      'apply_emp_id', 'apply_emp_name', 'check_amount', 'jzpz']
        columns_str = ",".join(columns_ls)

        # part1 查询异常数据的count
        x = datetime.now()
        sql = 'select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill where check_amount > jzpz limit 1000'.format(
            columns_str=columns_str)
        count_sql = 'select count(a.bill_id) from ({sql}) a'.format(sql=sql)
        log.info(count_sql)

        records = prod_execute_sql(sqltype='select', sql=count_sql)
        count_records = records[0][0]
        max_size = 1 * 1000
        limit_size = 100
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

        return select_sql_ls

    def query_records(self,select_sql, query_records_ls=[]):
        log.info('****** query_records ******')

        records = prod_execute_sql(sqltype='select', sql=select_sql)
        print(len(records), records)

        query_records_ls.append(records)


    def do_work(self):
        x = datetime.now()
        select_sql_ls = self.exec_select_data()
        print(select_sql_ls)
        log.info('查询共耗时' + str(datetime.now() - x))


        query_records_ls = []
        thread_pool = ThreadPoolExecutor(10)  # 定义5个线程执行此任务
        for select_sql in range(len(select_sql_ls)):
            thread_pool.submit(self.query_records, query_records_ls)

        print('--- part1 ok ---')
        print(len(query_records_ls), query_records_ls)



if __name__ == "__main__":
   check_03_work = CheckWork03()
   check_03_work.do_work()



