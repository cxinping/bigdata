# -*- coding: UTF-8 -*-
tmp_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

data = (i for i in tmp_list if i % 2 == 0)
for i in data:
    print("当前元素是：", i)

	
	