import numpy as np

a = (np.arange(16) * 2).reshape(4, 4)
print("原始数组:")
print(a)
b = np.array([[0, 1], [2, 3]])
c = np.array([[1, 2], [3, 3]])
print("两个维度都使用二维数组索引：")
print(a[b, c])
print("第一个维度都使用二维数组索引：")
print(a[b, 1])
print("第二个维度都使用二维数组索引：")
print(a[:, b])

