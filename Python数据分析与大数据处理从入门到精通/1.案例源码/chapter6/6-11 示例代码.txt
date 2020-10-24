# -*- coding: UTF-8 -*-

tmp_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
itor_list=iter(tmp_list)
print("使用next获取元素：",next(itor_list))
print("使用__next__获取元素：",itor_list.__next__())
for i in itor_list:
    print("当前元素是：",i)


