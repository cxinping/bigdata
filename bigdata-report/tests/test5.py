# -*- coding: utf-8 -*-
ls = [1,2,3,4,5]

def show(*ls):
    print(ls, type(ls))

show(*ls)

print('------------------------------')

list = ["a", "b", "c", "d", "e"]
for index, value in enumerate(list):
    print(index, value)