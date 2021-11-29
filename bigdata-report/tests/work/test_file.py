# -*- coding: utf-8 -*-

import os
from report.commons.test_hdfs_tools import HDFSTools as Test_HDFSTools
from report.commons.settings import CONN_TYPE
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED


def get_dest_file(year):
    dest_file = f"/you_filed_algos/prod_kudu_data/temp/travel_data_{year}.txt"
    return dest_file


def get_dest_file2(year):
    dest_file = f"/you_filed_algos/prod_kudu_data/temp/travel_data_rst_{year}.txt"
    return dest_file


def get_upload_hdfs_path(year):
    upload_hdfs_path = f'hdfs:///user/hive/warehouse/02_logical_layer_007_h_lf_cw.db/finance_travel_linshi_analysis/travel_data_exec_{year}.txt'
    return upload_hdfs_path


def exec_file(year):
    dict_file = get_dest_file(year)
    rst_dict_file = get_dest_file2(year)

    if os.path.exists(rst_dict_file):
        os.remove(rst_dict_file)
    os.mknod(rst_dict_file)

    fo = open(dict_file, "r", encoding='utf-8')
    content = ""
    while True:
        line = fo.readline()
        if not line:
            break

        temp = line.replace('，', ',')
        content = temp
        print(content)

        wo = open(rst_dict_file, "a+", encoding='utf-8')
        wo.write(content)


def upload_hdfs_file():
    test_hdfs = Test_HDFSTools(conn_type=CONN_TYPE)

    for year in ['2014', '2015', '2016', '2017', '2018', '2019', '2020', '2021']:
        dest_file = get_dest_file2(year)
        upload_hdfs_path = get_upload_hdfs_path(year)

        test_hdfs.uploadFile2(hdfsDirPath=upload_hdfs_path, localPath=dest_file)


def demo1():
    years = ['2019', '2020', '2021']
    max_workers = len(years)
    threadPool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="thr")
    all_task = [threadPool.submit(exec_file, year) for year in years]
    wait(all_task, return_when=ALL_COMPLETED)


def main():
    # exec_file('2014')
    #demo1()
    upload_hdfs_file()

    print('--- ok ---')


main()
