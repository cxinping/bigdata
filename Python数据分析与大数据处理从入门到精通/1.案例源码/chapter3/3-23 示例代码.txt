# -*- coding: UTF-8 -*-

tmp_list = [1, 2, 3.5 + 1j, 0x22, "Hello", "World"]
print("tmp_list类型是：", type(tmp_list))
print("tmp_list元素是：", tmp_list)

tmp_list1 = tmp_list[1:5]
print("tmp_list1类型是：", type(tmp_list1))
print("tmp_list1元素是：", tmp_list1)

tmp_list2 = list(range(5))
print("tmp_list2类型是：", type(tmp_list2))
print("tmp_list2元素是：", tmp_list2)


