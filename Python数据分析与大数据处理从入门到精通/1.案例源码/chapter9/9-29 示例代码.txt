import numpy as np

a = np.ones(5, dtype=np.int32)
print("数组a的类型：", a.dtype)
b = np.linspace(0, 10.5, 5)
print("数组b的类型：", b.dtype)
c = a * b
print("计算后的结果c的类型：", c.dtype)


