# -*- coding: UTF-8 -*-


def func1(a, b=5, *, c, d):
    print("输出位置参数a:%a,可选参数b:%s,\n命名关键字参数c:%s,命名关键字参数d:%s" % (a, b, c, d))


def func2(a, b=5, *args, c, d):
    print("输出位置参数a:%a,可选参数b:%s,\n命名关键字参数c:%s,命名关键字参数d:%s" % (a, b, c, d))


func1(10, c=11, d=12)
print("")
func2(10, c=11, d=12)


