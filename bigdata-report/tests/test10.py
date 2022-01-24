# -*- coding: utf-8 -*-

# import ariama

s1 = '''
111
222
333
'''

print(s1)

a1 = '")'
print(a1)

a1 = "\")"
print(a1)

alist = [1, 2, 3]
blist = [2, 3, 4, 5]

c = alist + blist
s = set(c)
c = list(s)
print(c)

print('#' * 30)

if not False:
    print('111')
else:
    print('222')

print('------------------------------------------')

import os

os.system('ps aux|grep python|grep -v grep|cut -c 9-15|xargs kill -15')






