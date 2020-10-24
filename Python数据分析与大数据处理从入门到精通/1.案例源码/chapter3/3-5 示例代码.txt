# -*- coding: UTF-8 -*-

val_int = 5555555555555555555555
del val_int
try:
    print(val_int)
except Exception as e:
    print("访问已删除对象，触发异常：", e)

print("给val_int变量重新赋值：")
val_int = 66666
print("正常显示val_int：", val_int)


