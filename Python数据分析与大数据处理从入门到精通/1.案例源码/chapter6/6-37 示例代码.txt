# -*- coding: UTF-8 -*-

try:
    a = 2/0
except ValueError as e:
    print("异常信息：",e)
finally:
    print("END")
	
	
	
	
	