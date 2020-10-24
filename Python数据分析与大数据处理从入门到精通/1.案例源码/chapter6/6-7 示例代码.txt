# -*- coding: UTF-8 -*-

tmp_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

def get_list():
    for i in tmp_list:
        if i % 2 == 0:
            print("当前元素是：", i)
            yield i

gen = get_list()
for j in gen:
    print("当前获取到的值：", j)
    print()

	
	