# -*- coding: UTF-8 -*-

tmp_list = [1, 2, 3.5 + 1j, 0x22, "Hello", "World"]
print("取出2-4范围数据：", tmp_list[2:5])
print("取出取出前3个数据：", tmp_list[:3])
print("取出第3个之后的数据：", tmp_list[3:])
print("在第1-6之间，每隔2个取一次：", tmp_list[1:6:2])
print("在整个列表范围内，每隔2个取一次：", tmp_list[::2])
print("取第0个到倒数第2个：", tmp_list[:-2])
print("将列表反向输出：", tmp_list[::-1])
print("在第5到第1个范围内，每隔两个取一次，反向取：", tmp_list[5:1:-2])


