#!/usr/bin/env python
# -*- coding: utf-8 -*-


def MyIterator():  # 定义一个迭代器函数
    for i, data in enumerate([1, 3, 9]):
        print("I'm in the idx:{0} call of next()".format(i))
        yield data


print(iter(MyIterator()), MyIterator())
for i in MyIterator():
    print(i)
