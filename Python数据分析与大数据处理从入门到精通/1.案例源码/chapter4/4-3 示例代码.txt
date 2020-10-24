# -*- coding: UTF-8 -*-

def add(item):
    print("在add中调用sub：", sub(item))
    return 5 + item

try:
    sub(5)
except Exception as e:
    print("在函数申明前调用，触发异常：", e)

def sub(item):
    return 10 - item

print("调用add方法：", add(5))


