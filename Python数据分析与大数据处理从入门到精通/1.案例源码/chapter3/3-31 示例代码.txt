# -*- coding: UTF-8 -*-

tmp_list = [1, 2, 3, 4, 5, 6]
tmp_list1 = [item for item in tmp_list if item % 2 == 0]
print("使用推导式：", tmp_list1)

tmp_list2 = []
for item in tmp_list:
    if item % 2 == 0:
        tmp_list2.append(item)

print("使用for循环：", tmp_list2)


def get_odd(item):
    if item % 2 == 0:
        return item
    else:
        return 0


tmp_list3 = [get_odd(item) for item in tmp_list]
print("推导式调用外部方法：", tmp_list3)


