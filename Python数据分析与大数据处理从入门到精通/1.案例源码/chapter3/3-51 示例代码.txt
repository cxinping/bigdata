# -*- coding: UTF-8 -*-

data_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
i = 0

total = len(data_list)
while i < total:
    if data_list[i] == 10:
        print("退出while循环")
        break
    elif data_list[i] == 8:
        print("执行elif语句")
    else:
        print("执行else语句，输出当前元素：", data_list[i])

    i = i + 1
	
	
	

	
	