# -*- coding: utf-8 -*-
from report.services.common_services import *
import csv


def demo1():
    province_service = ProvinceService()


def demo2():
    """
    行政区划代码   area_division_code
    省           province
    市           city
    县/区        county
    省代码       province_area
    市代码       city_area
    县/区代码    county_code

    如果纳税人代码识别号是10位，取前6位比对，如果是12位取3-9六位数字比对, 比对不出来的在根据省代码市代码县代码比对.
    比如河北省石家庄市长安区。 分别是13-河北省，01-石家庄市，02-长安区

    :return:
    """
    columns = ['area_division_code', 'province', 'city', 'county', 'province_area', 'city_area', 'county_code']
    dest_file = '/you_filed_algos/app/config/province_code.csv'
    f = csv.reader(open(file=dest_file, mode='r', encoding='gbk'))
    for idx, row in enumerate(f):
        if idx == 0:
            continue

        #print(idx, row)
        result_row = dict(zip(columns, row))
        print(result_row)

if __name__ == '__main__':
    demo2()
