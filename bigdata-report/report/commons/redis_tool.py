# -*- coding: utf-8 -*-
import redis

redis_connect = redis.StrictRedis(host='127.0.0.1', port=6379)

REPORT_KEY = 'report:area'

"""

SMEMBERS report:area  查看集合数目


"""

def save_area_record(record_dict):
    if record_dict:
        try:
            # 将 record_str 放入 cvsource_spider:enterprise_change_shareholders_seen_record
            sadd_result = redis_connect.sadd(REPORT_KEY, str(record_dict))
            print('sadd_result=', sadd_result)

        except Exception as e:
            print(str(e))

def query_area_scard():
    cnt = redis_connect.scard(REPORT_KEY)
    print(cnt)


if __name__ == "__main__":
    record_dict1 = {'name': '北京', 'longitude': 100, 'latitude': 120}
    record_dict2 = {'name': '河北', 'longitude': 110, 'latitude': 120}
    #save_area_record(record_dict2)
    query_area_scard()