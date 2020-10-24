# -*- coding: UTF-8 -*-

func1 = lambda x, y: x + y
print("lambda对象类型：", type(func1))
result = func1(1, 2)
print("匿名函数func1执行结果：", result)

def func2(x, y):
    return x + y

result = func2(1, 2)
print("函数func2执行结果：", result)


