# -*- coding: UTF-8 -*-

def func(a=None):
    if a is None:
        a = []
    a.append("hello")
    print("a是一个列表", a)


func()
func()
func()


