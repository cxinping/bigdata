# -*- coding: utf-8 -*-

"""
Created on Wed Nov 24 18:44:00 2021

@author: admin
"""

import re

a = ['长沙市德阳县安逸物资宾馆有限责任公司', '湖南省长沙市岳麓区车塘河路18号晚安床具工业基地9号栋综合楼 0731-83088881', '北京市朝阳区银行股份有限公司含浦支行800313432302010']

print(re.findall('.*省', a[1])[0])
print(re.findall('省.*市', a[1])[0].lstrip(r'省'))
print(re.findall('省.*区', a[1])[0].lstrip(r'省'))

c = re.findall('省.*区', a[1])[0].lstrip(r'省')

print(re.findall('市.*区', a[1])[0].lstrip(r'市'))


def match_address(place):
    zoneList = []
    province = re.findall('.*省', place)
    if province:
        zoneList.append(province[0])
    elif re.findall('.*自治区', place):
        zoneList.append(re.findall('.*自治区', place)[0])
    else:
        zoneList.append(None)

    city = re.findall('.*市', place)
    if city:
        zoneList.append(city[0].lstrip(zoneList[0]))
    else:
        zoneList.append(None)

    district = re.findall('市.*区', place)
    if district:
        zoneList.append(district[0].lstrip('市'))
    elif re.findall('市.*县', place):
        print(re.findall('市.*县', place))
        zoneList.append( re.findall('市.*县', place)[0])
    else:
        zoneList.append(None)

    return (zoneList)

def zoneGet(text):
    zoneList = []
    province = re.findall('.*省',text)
    if province:
        zoneList.append(province[0])
    elif re.findall('.*自治区',text):
        zoneList.append(re.findall('.*自治区',text)[0])
    else:
        zoneList.append('')

    city = re.findall('.*市',text)
    if city:
        zoneList.append(city[0].lstrip(zoneList[0]))
    else:
        zoneList.append('')

    district = re.findall('市.*区',text)
    if district:
        zoneList.append(district[0].lstrip('市'))
    else:
        zoneList.append('')
    return(zoneList)


r = zoneGet(a[0])
print('result =>', r)
print('*' * 50)
print(match_address(place=a[1]))
