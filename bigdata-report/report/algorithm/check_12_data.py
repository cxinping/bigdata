# -*- coding: utf-8 -*-

from report.commons.logging import get_logger
import os, time
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
import threading
from report.services.common_services import ProvinceService
from report.commons.test_hdfs_tools import HDFSTools as Test_HDFSTools
import pandas as pd
from report.commons.connect_kudu import prod_execute_sql
from report.commons.tools import (list_of_groups, kill_pid)
from report.services.common_services import query_billds_finance_all_targets

"""

SELECT bill_id, emp_name, origin_name, destin_name, beg_date, end_date, traf_name, 出差城市
FROM 01_datamart_layer_007_h_cw_df.finance_rma_travel_journey



"""

log = get_logger(__name__)

import sys

sys.path.append('/you_filed_algos/app')

dest_dir = '/you_filed_algos/prod_kudu_data/checkpoint12'
dest_file = dest_dir + '/check_12_data.txt'

conn_type = 'test'
test_limit_cond = ' '  # 'LIMIT 1000'``


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

        columns_ls = ['bill_id', 'emp_name', 'origin_name', 'destin_name', 'beg_date', 'end_date', 'traf_name' ]
        columns_str = ",".join(columns_ls)
        sql = 'select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_rma_travel_journey {test_limit_cond}'.format(
            columns_str=columns_str, test_limit_cond=test_limit_cond)

        count_sql = 'select count(a.bill_id) from ({sql}) a'.format(sql=sql)
        log.info(count_sql)
        records = prod_execute_sql(conn_type='test', sqltype='select', sql=count_sql)
        count_records = records[0][0]
        log.info(f'* count_records ==> {count_records}')

        max_size = 1 * 100000
        limit_size = 2 * 10000
        select_sql_ls = []

    def analyze_data_data(self):
        pass


if __name__ == "__main__":
    check12_service = Check12Service()
    check12_service.save_data()

    #check12_service.analyze_data_data()
