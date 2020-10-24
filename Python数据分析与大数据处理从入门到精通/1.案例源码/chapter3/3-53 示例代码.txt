# -*- coding: UTF-8 -*-

a = 1, 2, 3
print("对象a的类型：", type(a))


def test():
    return 4, 5, 6


b = test()
print("函数test返回的类型：", type(b))


