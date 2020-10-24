import numpy as np

a = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]).reshape(2, 3, 2)
print("原始数组：")
print(a)
print("三维数组的形状：", a.shape)
print("取第一维的第1行和余下的数据：", a[1, ...])


