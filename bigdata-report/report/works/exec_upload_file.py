# -*- coding: utf-8 -*-
from report.commons.hdfs_tools import HDFSTools as Prod_HDFSTools


def upload_finance_hdfs():
    """
    上传行政区划表到生产环境
    :return:
    """
    prod_hdfs = Prod_HDFSTools(conn_type='prod')
    hdfs_file_url = 'hdfs:///user/hive/warehouse/02_logical_layer_007_h_lf_cw.db/finance_administration/'
    local_file_name = '/you_filed_algos/app/doc/province_code.txt'

    prod_hdfs.uploadFile(hdfsDirPath=hdfs_file_url, localPath=local_file_name)


if __name__ == "__main__":
    upload_finance_hdfs()
