import numpy as np

a = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]).reshape(3, 4)
print("原始数组：")
print(a)
print("没有提供第二个维度的索引，选取的数据是：", a[2])


