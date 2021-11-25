# -*- coding: utf-8 -*-
from report.services.common_services import *
import csv
import os
from report.commons.test_hdfs_tools import HDFSTools as Test_HDFSTools


def demo1():
    province_service = ProvinceService()


dest_file = '/you_filed_algos/app/report/works/finance_administration.txt'
upload_hdfs_path = '/user/hive/warehouse/02_logical_layer_007_h_lf_cw.db/finance_administration/finance_administration.txt'


def init_file():
    """
    行政区划代码   area_division_code
    省           province
    市           city
    县/区        county
    省代码       province_code
    市代码       city_code
    县/区代码    county_code

    如果纳税人代码识别号是10位，取前6位比对，如果是12位取3-9六位数字比对, 比对不出来的在根据省代码市代码县代码比对.
    比如河北省石家庄市长安区。 分别是13-河北省，01-石家庄市，02-长安区

    :return:
    """
    columns = ['area_division_code', 'province', 'city', 'county', 'province_area', 'city_area', 'county_code']
    sour_file = '/you_filed_algos/app/report/works/province_code.csv'

    if os.path.exists(dest_file):
        os.remove(dest_file)

    f = csv.reader(open(file=sour_file, mode='r', encoding='gbk'))
    for idx, row in enumerate(f):
        if idx == 0:
            continue

        # print(idx, row)
        # result_row = dict(zip(columns, row))
        print(row)
        area_division_code = row[0]
        province = row[1]
        city = row[2]
        county = row[3]
        province_code = row[4]
        city_code = row[5]
        county_code = row[6]

        area_division_code = area_division_code if area_division_code else '无'
        province = province if province else '无'
        city = city if city else '无'
        county = county if county else '无'
        province_code = province_code if province_code else '无'
        city_code = city_code if city_code else '无'
        county_code = county_code if county_code else '无'

        line = f'{area_division_code},{province},{city},{county},{province_code},{city_code},{county_code}'
        # print(line)

        if area_division_code == '无':
            break

        with open(dest_file, "a+", encoding='utf-8') as file:
            file.write(line + "\n")


def upload_file():
    test_hdfs = Test_HDFSTools(conn_type='test')
    test_hdfs.uploadFile2(hdfsDirPath=upload_hdfs_path, localPath=dest_file)


def demo2():
    s = 'abcdefhighlmn'
    print(s[1:3])

    s2 = '1234567'
    print(s2[0:2])
    print(s2[2:4])
    print(s2[4:6])


def demo3():
    finance_service = FinanceAdministrationService()
    # area_division_code = '310114'
    # rst = finance_service.query_accurate_areas(area_division_code)
    # print(rst, len(rst))

    # area_division_code = '31aabb'
    # rst = finance_service.query_blur_areas(area_division_code)
    # print(rst, len(rst))

    sales_taxno = '210104aaaaaaaaaaaaaa'
    rst = finance_service.query_areas(sales_taxno=sales_taxno)
    print(rst, len(rst))


if __name__ == '__main__':
    # init_file()
    # upload_file()

    # demo2()
    demo3()
