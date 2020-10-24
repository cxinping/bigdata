# -*- coding: UTF-8 -*-
dic = {}
print("字典的类型：", type(dic))
print("字典的内容：", dic)
dic = {"key1": "hello", "key2": "world"}
print("创建字典时设置初始值：", dic)
dic = dict((["key3", "hello"], ["key4", "world"]))
print("使用dict关键字创建字典：", dic)
dic = {}.fromkeys(("key1", "key2"), ("hello", "world"))
print("使用fromkeys内建函数创建字典：", dic)


