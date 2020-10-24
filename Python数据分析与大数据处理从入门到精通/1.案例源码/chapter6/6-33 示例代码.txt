# -*- coding: UTF-8 -*-

def get_exception():
    raise

def get_exception1():
    get_exception()

def get_exception2():
    get_exception1()

try:
    get_exception2()
except Exception as e:
    print("异常信息：", e)
finally:
    print("异常捕获最后执行的代码!")
	
	
	
