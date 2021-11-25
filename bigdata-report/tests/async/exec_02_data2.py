# -*- coding: utf-8 -*-

from report.commons.logging import get_logger
from report.commons.test_hdfs_tools import HDFSTools as Test_HDFSTools


log = get_logger(__name__)

dest_file = "/you_filed_algos/app/doc/finance_province_city.txt"
upload_hdfs_path = 'hdfs:///user/hive/warehouse/02_logical_layer_007_h_lf_cw.db/finance_province_city/finance_province_city.txt'

conn_type = 'prod' # test prod
test_hdfs = Test_HDFSTools(conn_type=conn_type)
test_hdfs.uploadFile2(hdfsDirPath=upload_hdfs_path, localPath=dest_file)


