import numpy as np

a = np.array([7, 6, 5, 4, 3, 10, 12, 15]).reshape(2, 4)
print("二维数组a：")
print(a)
b = np.array([1, 2, 3, 4])
c = a + b
print("二维数组加一维数组：")
print(c)


