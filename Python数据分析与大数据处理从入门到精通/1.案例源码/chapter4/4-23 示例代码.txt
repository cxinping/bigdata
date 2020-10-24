# -*- coding: UTF-8 -*-

def func1(*args):
    count = 0
    for i in args:
        count = count + i
    return count


def func2(**kwargs):
    tmp_list = []
    for k, v in kwargs.items():
        tmp_list.append("key:%s   value:%s" % (k, v))
    return tmp_list


data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
print("传递可变参数：", func1(*data))
data = (1, 3, 5, 7, 9)
print("传递可变参数：", func1(*data))
dic = {"key1": 1, "key2": 2, "key3": 3, "key4": 4}
print("传递关键字参数：", func2(**dic))


