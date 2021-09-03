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

if __name__ == '__main__':
    #data = str(input("请输入文本:"))
    data = "新疆机场集团天缘酒店管理有限责任公司库尔勒市天缘商务酒店"
    #data = '贵州省黔南州贵定县'
    province = match_address(place=data,key='市')
    print(province )






