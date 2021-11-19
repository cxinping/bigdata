# -*- coding: utf-8 -*-
str1 = "aaabbbcccddd公司eeefffff1111"
str2 = "公司"

idx = str1.find(str2)+2
print(idx)
print(str1[idx:])
