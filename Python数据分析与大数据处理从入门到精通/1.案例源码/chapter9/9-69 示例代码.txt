import numpy as np

a = np.arange(10).reshape(2, 5)
print(a)

b = np.ix_([0, 1], [2, 3])
print("使用整数数组筛选数据：")
print(a[b])
c = np.ix_([True, True], [1, 3])
print("使用布尔数组筛选数据：")
print(a[c])



