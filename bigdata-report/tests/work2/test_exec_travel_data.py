# -*- coding: utf-8 -*-

import sys
from report.commons.logging import get_logger
import time
import os
from report.commons.connect_kudu2 import prod_execute_sql
from report.commons.test_hdfs_tools import HDFSTools as Test_HDFSTools
from report.commons.tools import MatchArea, process_invalid_content, is_chinese
from report.services.common_services import ProvinceService, FinanceAdministrationService

from report.works.full_add.exec_travel_data_gevent import *

log = get_logger(__name__)

dest_dir = '/you_filed_algos/prod_kudu_data/temp'

test_limit_cond = ' '  # 'LIMIT 10000'


def get_dest_file(year):
    dest_file = f"/you_filed_algos/prod_kudu_data/temp/travel_data_{year}.txt"
    return dest_file


def get_upload_hdfs_path(year):
    upload_hdfs_path = f'hdfs:///user/hive/warehouse/02_logical_layer_007_h_lf_cw.db/finance_travel_linshi_analysis/travel_data_{year}.txt'
    return upload_hdfs_path


def init_file(year):
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)

    dest_file = get_dest_file(year)
    if os.path.exists(dest_file):
        os.remove(dest_file)

    os.mknod(dest_file)


def test_execute_02_data(year):
    init_file(year)

    columns_ls = ['destin_name', 'sales_name', 'sales_addressphone', 'sales_bank', 'finance_travel_id', 'origin_name',
                  'invo_code', 'sales_taxno']

    columns_str = ",".join(columns_ls)
    sql = """
    select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill 
    where  finance_travel_id in ("36e19b94-e91e-44e6-b22c-45a1afe550c4" )
       {test_limit_cond}
    """.format(columns_str=columns_str, year=year, test_limit_cond=test_limit_cond)
    sql = sql.replace('\n', '').replace('\r', '').strip()

    print('====================================================================')
    log.info(sql)

    exec_task(sql, year)

    # 上传文件到HDFS
    upload_hdfs_file(year)

    # 刷新临时表
    refresh_linshi_table()


if __name__ == "__main__":
    year = '2021'
    test_execute_02_data(year)





