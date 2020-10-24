# -*- coding: UTF-8 -*-

def func(item):
    if item % 2 == 0:
        return True

data1 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

data2 = filter(func, data1)
print("输出filter返回值类型：", type(data2))
print("将filter对象转为列表：", list(data2))


