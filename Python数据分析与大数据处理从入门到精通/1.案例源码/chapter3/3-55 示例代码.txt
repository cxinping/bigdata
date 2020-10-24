# -*- coding: UTF-8 -*-

a = 1, 2, 3
copy_a = tuple(a)
print("元组a：", id(a))
print("元组a副本：", id(copy_a))

b = [1, 2, 3]
copy_b = list(b)
print("列表b：", id(b))
print("列表b副本：", id(copy_b))

c = range(100000)
list_c = list(c)
print("列表内存块大小：", list_c.__sizeof__())
tuple_c = tuple(c)
print("元组内存块大小：", tuple_c.__sizeof__())


