# -*- coding: utf-8 -*-
import os
import time

from report.commons.connect_kudu import prod_execute_sql
from report.commons.logging import get_logger
from report.commons.test_hdfs_tools import HDFSTools as Test_HDFSTools
from report.commons.tools import match_address, split_str



log = get_logger(__name__)

dest_file = "/you_filed_algos/app/doc/finance_province_city.txt"
upload_hdfs_path = '/user/hive/warehouse/02_logical_layer_007_h_lf_cw.db/finance_province_city/'


test_hdfs = Test_HDFSTools(conn_type='test')
test_hdfs.uploadFile2(hdfsDirPath=upload_hdfs_path, localPath=dest_file)
