# -*- coding: utf-8 -*-
import requests
import json
from math import radians, cos, sin, asin, sqrt


def query_longitude_latitude(url):
    """
    longitude 经度
    latitude 纬度

    :param url:
    :return:
    """

    try:
        r = requests.get(url)
        # print(r.text)

        data = json.loads(r.text)
        districts = data['districts']

        if len(districts) > 0:
            district = districts[0]
            center = district['center']
            if center:
                longitude_latitude = center.split(',')
                longitude = longitude_latitude[0]
                latitude = longitude_latitude[1]
                return longitude, latitude
        else:
            return None, None
    except Exception as e:
        print(e)
        return None, None


def distance(lon1, lat1, lon2, lat2):  # 经度1，纬度1，经度2，纬度2 （十进制度数）
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # 将十进制度数转化为弧度
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine公式
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    r = 6371  # 地球平均半径，单位为公里
    return c * r * 1000


if __name__ == '__main__':
    url = 'https://restapi.amap.com/v3/config/district?keywords={area}&subdistrict=0&key={key}&extensions=base'.format(
        key='0e540c9f3f92b59a54529966d3e13e27', area='北京')
    # r = query_longitude_latitude(url)
    # print(r)

    r2 = distance(22.599578, 113.973129, 22.6986848, 114.3311032)
    print(f'两个经纬度相距 {r2} 公里')
