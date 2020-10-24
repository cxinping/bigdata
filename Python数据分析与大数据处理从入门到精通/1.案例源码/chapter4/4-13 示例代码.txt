# -*- coding: UTF-8 -*-

def func1(a):
    print("输出位置参数a的值：", a)
    return a


def func2(a, b):
    print("输出位置参数a:%a,b:%s" % (a, b))
    return a + b


print("函数调用func1(10):", func1(10))
print("")
print("函数调用func2(10,20):", func2(10, 20))


