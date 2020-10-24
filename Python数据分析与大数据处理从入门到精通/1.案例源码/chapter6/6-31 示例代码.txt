# -*- coding: UTF-8 -*-
def get_exception():
    raise

def get_exception1():
    get_exception()

def get_exception2():
    get_exception1()


get_exception2()


