# -*- coding: UTF-8 -*-

tmp_list = [1, 2, 3.5 + 1j, 0x22, "Hello", "World"]

print("获取指定元素：", tmp_list[2])
print("元素修改前：", id(tmp_list))
tmp_list[2] = 88
print("元素修改后：", id(tmp_list))
print("修改元素后的列表：", tmp_list)
del tmp_list[2]
print("删除元素后的列表：", tmp_list)


