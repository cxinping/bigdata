# -*- coding: UTF-8 -*-
data1 = [6, 4, 10, 3, 9, 2, 8, 5, 7, 1]
print("对列表排序,默认升序：", sorted(data1))
print("对列表降序排列：", sorted(data1, reverse=True))
data2 = [-10, -1, 0, 30, 28, 15]
print("使用key参数对每个元素按绝对值排序：", sorted(data2, key=abs))


