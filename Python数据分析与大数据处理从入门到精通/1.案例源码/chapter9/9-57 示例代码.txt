import numpy as np

a = np.arange(10) * 2
print("原始数组：", a)
b = np.array([1, 1, 3, 4])
print("通过b索引的数据：", a[b])
c = np.array([[2, 3], [5, 6]])
print("通过c索引的数据：", a[c])


