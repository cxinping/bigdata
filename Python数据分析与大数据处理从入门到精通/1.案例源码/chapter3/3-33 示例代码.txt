# -*- coding: UTF-8 -*-
tuple1 = ("hello", "world", "python", "spark", "hadoop")
print("获取指定位置的元素：", tuple1[0])

tuple2 = (-10, -20, 5.888, 888, 20000)
print("元组正向切片：", tuple2[1:])

tuple3 = ("hello", [1, 2], -20, 888, 5.888)
print("元组反向切片：", tuple3[:-1])

tuple4 = ([1, 2], [3, 4], [5, 6, 7, 8], [9, 10, 11, 12, 13, 14])
print("获取列表中的元素：", tuple4[2][:-1])


