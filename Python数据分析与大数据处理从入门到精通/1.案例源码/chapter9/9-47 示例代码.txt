import numpy as np

a = np.floor(10 * np.random.random((2)))
print("数组a:")
print(a)
b = np.floor(10 * np.random.random((2)))
print("数组b:")
print(b)
print("沿垂直方向堆叠：")
c = np.column_stack((a, b))
print(c)
print("添加新轴然后进行堆叠：")
d = np.column_stack((a[:, np.newaxis], b[:, np.newaxis]))
print(d)


