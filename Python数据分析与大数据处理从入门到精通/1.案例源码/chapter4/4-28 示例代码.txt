# -*- coding: UTF-8 -*-

def func(item):
    return item + 1

data1 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

data2 = map(func, data1)
print("输出map返回值类型：",type(data2))
print("将map对象转为元组：",tuple(data2))



