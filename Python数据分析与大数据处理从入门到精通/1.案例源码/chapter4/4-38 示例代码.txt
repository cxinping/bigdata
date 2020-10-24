# -*- coding: UTF-8 -*-

data1 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
data2 = map(lambda item: item + 1, data1)
print("将map对象转为元组：", tuple(data2))
print()
data1 = [("a", -1), ("b", 16), ("c", 12), ("d", 18), ("e", -5)]
print("使用key参数对元组元素排序：", sorted(data1, key=lambda x: x[1]))
print()
data2 = [{"key": -1}, {"key": 16}, {"key": 12}, {"key": 18}, {"key": -5}]
print("使用key参数对字典元素排序：", sorted(data2, key=lambda x: x["key"]))


