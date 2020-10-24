import numpy as np

a = np.floor(10 * np.random.random((2, 10)))
print("数组a:")
print(a)
b = np.floor(10 * np.random.random((2, 10)))
print("数组b:")
print(b)
print("沿垂直方向堆叠：")
c = np.vstack((a, b))
print(c)
print("沿水平方向堆叠")
d = np.hstack((a, b))
print(d)


