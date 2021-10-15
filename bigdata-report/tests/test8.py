# -*- coding: utf-8 -*-
from report.commons.tools import not_empty



str1 = '1234567890'
print(str1.find('111'))

str2 ='*汽油*车用油'
str3= '*汽油*95号车用汽油（VIA）,*汽油*92号车用汽油（VIA）'
str_ls = str3.split(',')
print(str_ls, type(str_ls))

str_ls = list(filter(not_empty, str_ls))
print(str_ls, type(str_ls))
print(str_ls[0])
