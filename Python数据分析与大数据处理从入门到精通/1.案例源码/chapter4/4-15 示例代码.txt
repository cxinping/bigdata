# -*- coding: UTF-8 -*-

def func2(a, b=5):
    print("输出位置参数a:%a,可选参数b:%s" % (a, b))
    return a + b


print("调用时传递1个参数func2(1):", func2(1))
print("")
print("调用时传递2个参数func2(1,2):", func2(1, 2))


