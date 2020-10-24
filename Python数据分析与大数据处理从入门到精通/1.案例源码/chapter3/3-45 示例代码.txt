# -*- coding: UTF-8 -*-

dic = {"key1": "hello", "key2": "world"}
print("所有的key：", dic.keys())
print("所有的值：", dic.values())

for item in dic.items():
    print("当前项：", item)

cur_item = dic.get("key1")
print("key1的值：", cur_item)

dic.update({"key1": "python"})
print("使用update修改字典：", dic)

cur_val = dic.pop("key1")
print("pop返回当前键对应值并从字典中移除数据：", cur_val)
print("调用pop方法后的字典：", dic)

dic.clear()
print("清空字典所有内容：", dic)


