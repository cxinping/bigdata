# -*- coding: utf-8 -*-


import re

a = ['湛江市坡头区合作路107号', '海口市秀英区西海岸长滨三路6号0898 0731-83088881', '北京市朝阳区银行股份有限公司含浦支行800313432302010']

firstRegion = ['省', '自治区']
secondRegion = ['市', '自治州', '盟']
thirdRegion = ['区', '县', '自治旗']

n = len(firstRegion)
m = len(secondRegion)
l = len(thirdRegion)


def match_address(place):
    firstRegion = ['省', '自治区']
    secondRegion = ['市', '自治州', '盟']
    thirdRegion = ['区', '县', '自治旗']

    n = len(firstRegion)
    m = len(secondRegion)
    l = len(thirdRegion)

    i = 0
    j = 0
    k = 0
    res = []
    area_name = None

    while i in range(n):
        split = place.split(firstRegion[i])
        if len(split) > 1:
            res.append(split[0] + firstRegion[i])
            place = split[1]
        elif len(res) < 1 and i == n - 1:
            res.append(None)
        i = i + 1

    while j in range(m):
        split = place.split(secondRegion[j])
        if len(split) > 1:
            res.append(split[0] + secondRegion[j])
            place = split[1]
        elif len(res) < 2 and j == m - 1:
            res.append(None)
        j = j + 1

    while k in range(l):
        split = place.split(thirdRegion[k])
        if len(split) > 1:
            res.append(split[0] + thirdRegion[k])
            place = split[1]
        elif len(res) < 3 and k == l - 1:
            res.append(None)
        k = k + 1
    return res

def match_address2(place):
    i = 0
    j = 0
    k = 0
    res = []
    while i in range(n):
        split = place.split(firstRegion[i])
        if len(split) > 1:
            res.append(split[0] + firstRegion[i])
            place = split[1]
        elif len(res) < 1 and i == n - 1:
            res.append(None)
        i = i + 1

    while j in range(m):
        split = place.split(secondRegion[j])
        if len(split) > 1:
            res.append(split[0] + secondRegion[j])
            place = split[1]
        elif len(res) < 2 and j == m - 1:
            res.append(None)
        j = j + 1

    while k in range(l):
        split = place.split(thirdRegion[k])
        if len(split) > 1:
            res.append(split[0] + thirdRegion[k])
            place = split[1]
        elif len(res) < 3 and k == l - 1:
            res.append(None)
        k = k + 1
    return (res)


if __name__ == "__main__":
    print('result is:', match_address(place=a[0] ))





