# -*- coding: UTF-8 -*-

def func2(a, b=5, c=6, d=7):
    print("输出位置参数a:%a,可选参数b:%s,可选参数c:%s,可选参数d:%s" % (a, b, c, d))
    return a + b + c + d


print("参数按顺序赋值:", func2(1, 2, 3, 4))
print("")
print("参数按指定名称赋值:", func2(1, d=2, b=4))


