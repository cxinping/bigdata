# -*- coding: UTF-8 -*-

dic = {"key1": "hello", "key2": "world"}
print("获取key1的值：", dic["key1"])
dic["key1"] = "hello python"
print("修改key1的内容：", dic)
dic["key2"] = list(range(0, 5))
print("修改修改key2的内容：", dic)
dic["key3"] = {"key4": "spark"}
print("添加一个新键key3：", dic)
del dic["key2"]
print("删除key2后的内容：", dic)


