# -*- coding: utf-8 -*-
from report.commons.hdfs_tools import HDFSTools as Prod_HDFSTools
from report.commons.test_hdfs_tools import HDFSTools as Test_HDFSTools
import shutil, os, time
from datetime import datetime


def main():
    prod_hdfs = Prod_HDFSTools(conn_type='prod')
    # 递归下载 HDFS 上的文件夹里的文件
    # /user/hive/warehouse/02_logical_layer_001_o_lf_cw.db/occw0101_m hdfs:///user/hive/warehouse/02_logical_layer_001_o_lf_cw.db/occw0101_m
    # /user/hive/warehouse/03_basal_layer_zfybxers00.db hdfs:///user/hive/warehouse/03_basal_layer_zfybxers00.db
    hdfsDirUrl = 'hdfs:///user/hive/warehouse/03_basal_layer_zfybxers00.db'
    localDirUrl = '/my_filed_algos/prod_kudu_data/'

    print('* part1 ttt ')
    hdfsFileUrl_ls = prod_hdfs.downLoadDir_recursion(hdfsDirUrl=hdfsDirUrl,
                                                     localDirUrl=localDirUrl)
    print('* part2 ')
    print('*** 处理文件数 ==> ', len(hdfsFileUrl_ls))

    if os.path.exists(localDirUrl + 'user'):
        shutil.rmtree(localDirUrl + 'user')

    test_hdfs = Test_HDFSTools(conn_type='test')

    x = datetime.now()
    for index, hdfs_file_url in enumerate(hdfsFileUrl_ls):
        hdfs_file_url = str(hdfs_file_url)
        print(f'处理HDFS文件 {len(hdfsFileUrl_ls)} , hdfsFileUrl_ls index => {index}')
        print('prod hdfs_file_url => ', hdfs_file_url)
        local_file_name = hdfs_file_url.replace('hdfs://nameservice1/', localDirUrl)
        print('local_file_name => ', local_file_name)
        hdfs_file_url = hdfs_file_url.replace('hdfs://nameservice1/user', 'hdfs:///user')
        print('test hdfs_file_url => ', hdfs_file_url)

        # time.sleep(2)
        prod_hdfs.downLoadFile(hdfs_file_url, local_file_name)
        # time.sleep(2)
        test_hdfs.uploadFile(hdfsDirPath=hdfs_file_url, localPath=local_file_name)

        if os.path.exists(local_file_name):
            os.remove(local_file_name)
            print(f'delete file {local_file_name}')

        print('')

    print('共耗时' + str(datetime.now() - x))

    prod_hdfs.shutdownJVM()
    print('--- ok , completed work ---')


if __name__ == "__main__":
    main()
