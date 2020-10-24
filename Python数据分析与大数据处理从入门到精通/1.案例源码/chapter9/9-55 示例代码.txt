import numpy as np

a = np.arange(16)
b = a.copy()
if b is a:
    print("b和a是同一个对象")
else:
    print("b和a不是同一个对象")

b[5] = 200
print("数组b：", b)
print("数组a：", a)


