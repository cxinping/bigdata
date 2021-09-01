# -*- coding: utf-8 -*-
import time
import datetime
import re
from report.commons.logging import get_logger

log = get_logger(__name__)


def match_Address(data):
    PATTERN = r'([\u4e00-\u9fa5]{2,5}?(?:省|自治区|市)){0,1}([\u4e00-\u9fa5]{2,7}?(?:区|县|州)){0,1}([\u4e00-\u9fa5]{2,7}?(?:镇)){0,1}([\u4e00-\u9fa5]{2,7}?(?:村|街|街道)){0,1}([\d]{1,3}?(号)){0,1}'

    # \u4e00-\u9fa5 匹配任何中文
    # {2,5} 匹配2到5次
    # ? 前面可不匹配
    # (?:pattern) 如industr(?:y|ies) 就是一个比 'industry|industries' 更简略的表达式。意思就是说括号里面的内容是一个整体是以y或者ies结尾的单词
    pattern = re.compile(PATTERN)
    province = ''
    city = ''
    p3 = ''
    p4 = ''
    p5 = ''
    p6 = ''
    m = pattern.search(data)
    print(m.lastindex)
    if not m:
        print('None')
    if m.lastindex >= 1:
        province = m.group(1)
    if m.lastindex >= 2:
        city = m.group(2)
    if m.lastindex >= 3:
        p3 = m.group(3)
    if m.lastindex >= 4:
        p4 = m.group(4)
    if m.lastindex >= 5:
        p5 = m.group(5)
    if m.lastindex >= 6:
        p6 = m.group(6)
    #out = '%s|%s|%s|%s|%s|%s' % (province, city, p3, p4, p5, p6)
    return province, city

if __name__ == '__main__':
    #data = str(input("请输入文本:"))
    data = "安徽省淮南县大通区大通街道某某某"
    #data = '贵州省黔南州贵定县'
    province, city = match_Address(data)
    print(province, city)
    idx = city.find('县')

    xian_city = city[0:city.find('县')+1]
    print(xian_city)




