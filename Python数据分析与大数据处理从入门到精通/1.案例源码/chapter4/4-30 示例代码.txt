# -*- coding: UTF-8 -*-
from functools import reduce

def func(item1, item2):
    return item1 + item2

data1 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

data2 = reduce(func, data1)
print("序列求和：", data2)
data2 = reduce(func, data1, 10000)
print("序列求和：", data2)




