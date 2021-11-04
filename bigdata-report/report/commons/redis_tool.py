# -*- coding: utf-8 -*-
import redis

r = redis.Redis(host='localhost', port=6379, decode_responses=True)
r.set('name', 'runoob')  # 设置 name 对应的值
print(r['name'])


