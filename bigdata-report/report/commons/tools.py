# -*- coding: utf-8 -*-
import time
import datetime
import re
from report.commons.logging import get_logger

log = get_logger(__name__)


def match_address(place , key):

    ssxq = ['省', '市', '县', '区']
    indexes0 = [place.find(x) for x in ssxq]
    indexes = [x for x in indexes0 if x > 0]
    indexes2 = sorted(indexes)
    indexes2.insert(0, -1)
    address = []

    for i, x in enumerate(indexes2):
        if i == len(indexes2) - 1:
            continue
        else:
            address.append(place[x + 1:indexes2[i + 1] + 1])

    for addr in address:
        if addr.find(key) > -1:
            return addr
    return None

def check_invoicing_place(addr1,  addr2):
    if addr1.find(addr2) > -1 or addr2.find(addr1):
        return True

    return False

def read_file(path):
    with open(path, "r") as f:
        data = f.read()
        return data

if __name__ == '__main__':
    #data = str(input("请输入文本:"))
    #data = "安徽安庆市大观区经三路3号 0556-5386666"
    data = '江苏省无锡市滨湖区环湖路188号0510'
    province = match_address(place=data,key='市')
    print(province )
    key = '无锡'
    print(province.find(key))

    str1 ='北京市,杭州市,衢州市,郑州市,安庆市,洛阳市'
    str2 = '安庆市'
    print(str1.find(str2))

    path = "/you_filed_algos/prod_kudu_data/123.txt"
    content = read_file(path)
    print(content)




