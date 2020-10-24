# -*- coding: UTF-8 -*-
import sys
sys.setrecursionlimit(2000)
def add(item):
    if item == 1:
        return 1
    return item + add(item - 1)


result = add(999)
print(result)


