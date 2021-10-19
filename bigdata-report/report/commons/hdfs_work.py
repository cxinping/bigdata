# -*- coding: utf-8 -*-
from concurrent.futures import ThreadPoolExecutor, as_completed

import os
import shutil
import time
from datetime import datetime

from report.commons.hdfs_tools import HDFSTools as Prod_HDFSTools
from report.commons.test_hdfs_tools import HDFSTools as Test_HDFSTools


def main1():
    prod_hdfs = Prod_HDFSTools(conn_type='prod')
    # 递归下载 HDFS 上的文件夹里的文件
    # /user/hive/warehouse/02_logical_layer_001_o_lf_cw.db/occw0101_m hdfs:///user/hive/warehouse/02_logical_layer_001_o_lf_cw.db/occw0101_m
    # hdfs:///user/hive/warehouse/03_basal_layer_zfybxers00.db/zfybxers00_z_rma_bill_expense_m

    hdfsDirUrl = 'hdfs:///user/hive/warehouse/03_basal_layer_vms00.db/vms00_z_vms_invspecial_row_m'
    localDirUrl = '/my_filed_algos/prod_kudu_data/'

    print('* part1 better ')
    hdfsFileUrl_ls = prod_hdfs.downLoadDir_recursion(hdfsDirUrl=hdfsDirUrl,
                                                     localDirUrl=localDirUrl)
    print('* part2 ')
    print('*** 需要处理HDFS文件数 ==> ', len(hdfsFileUrl_ls))

    if os.path.exists(localDirUrl + 'user'):
        shutil.rmtree(localDirUrl + 'user')

    test_hdfs = Test_HDFSTools(conn_type='test')

    x_all = datetime.now()
    for index, hdfs_file_url in enumerate(hdfsFileUrl_ls):
        x = datetime.now()
        hdfs_file_url = str(hdfs_file_url)
        print(f'处理HDFS文件 {len(hdfsFileUrl_ls)} , hdfsFileUrl_ls index => {index}')
        print('prod hdfs_file_url => ', hdfs_file_url)
        local_file_name = hdfs_file_url.replace('hdfs://nameservice1/', localDirUrl)
        print('local_file_name => ', local_file_name)
        hdfs_file_url = hdfs_file_url.replace('hdfs://nameservice1/user', 'hdfs:///user')
        print('test hdfs_file_url => ', hdfs_file_url)

        time.sleep(0.01)
        prod_hdfs.downLoadFile2(hdfs_file_url, local_file_name)
        # time.sleep(1)
        test_hdfs.uploadFile2(hdfsDirPath=hdfs_file_url, localPath=local_file_name)

        if os.path.exists(local_file_name):
            os.remove(local_file_name)
            print(f'delete file {local_file_name}')

        print('一个操作HDFS文件共耗时' + str(datetime.now() - x))
        print('')

    print('共耗时' + str(datetime.now() - x_all))
    print('--- ok , completed work ---')
    prod_hdfs.shutdownJVM()


def main2():
    prod_hdfs = Prod_HDFSTools(conn_type='prod')
    # 递归下载 HDFS 上的文件夹里的文件
    # /user/hive/warehouse/03_basal_layer_vms00.db  hdfs:///user/hive/warehouse/03_basal_layer_vms00.db
    # /user/hive/warehouse/02_logical_layer_004_d_lf_cw.db/dccw0101_m hdfs:///user/hive/warehouse/02_logical_layer_004_d_lf_cw.db/dccw0101_m
    hdfsDirUrl = 'hdfs:///user/hive/warehouse/03_basal_layer_vms00.db/vms00_z_vms_invspecial_row_m'
    localDirUrl = '/my_filed_algos/prod_kudu_data/'

    print('* part1 ')
    hdfsFileUrl_ls = prod_hdfs.downLoadDir_recursion(hdfsDirUrl=hdfsDirUrl,
                                                     localDirUrl=localDirUrl)
    print('* part2 ')
    print('*** 处理文件数 ==> ', len(hdfsFileUrl_ls))

    if os.path.exists(localDirUrl + 'user'):
        shutil.rmtree(localDirUrl + 'user')

    test_hdfs = Test_HDFSTools(conn_type='test')

    # 多线程 1，从生产集群下载文件 2, 向开发集群上传文件
    threadPool = ThreadPoolExecutor(max_workers=50)
    x = datetime.now()
    obj_list = []

    for index, hdfs_file_url in enumerate(hdfsFileUrl_ls):
        x = datetime.now()
        hdfs_file_url = str(hdfs_file_url)
        print(f'处理HDFS文件 {len(hdfsFileUrl_ls)} , hdfsFileUrl_ls index => {index}')
        print('prod hdfs_file_url => ', hdfs_file_url)
        local_file_name = hdfs_file_url.replace('hdfs://nameservice1/', localDirUrl)
        print('local_file_name => ', local_file_name)
        hdfs_file_url = hdfs_file_url.replace('hdfs://nameservice1/user', 'hdfs:///user')
        print('test hdfs_file_url => ', hdfs_file_url)

        obj = threadPool.submit(exec_task, prod_hdfs, test_hdfs, hdfs_file_url, local_file_name)
        obj_list.append(obj)

    for future in as_completed(obj_list):
        data = future.result()
        print(data)

    threadPool.shutdown(wait=True)
    print('共耗时' + str(datetime.now() - x))
    print('--- ok , completed work ---')
    prod_hdfs.shutdownJVM()


def exec_task(prod_hdfs, test_hdfs, hdfs_file_url, local_file_name):
    prod_hdfs.downLoadFile2(hdfs_file_url, local_file_name)
    test_hdfs.uploadFile2(hdfsDirPath=hdfs_file_url, localPath=local_file_name)
    if os.path.exists(local_file_name):
        os.remove(local_file_name)
        print(f'delete file {local_file_name}')

    return f'upload file from {local_file_name} to {hdfs_file_url}'


if __name__ == "__main__":
    main1()

    #main2()

    pass
