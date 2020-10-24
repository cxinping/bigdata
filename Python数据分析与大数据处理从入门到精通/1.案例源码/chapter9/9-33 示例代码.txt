import numpy as np

a = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]).reshape(3, 4)
print("原始数组：")
print(a)
print("按下标选择元素：", a[2, 3])
print("按第行切片选择指定列：", a[0:3, 2])
print("按行切片选择所有列：", a[0:2, :])
print("按列切片选择所有行：", a[:, 1:3])


