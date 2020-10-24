import numpy as np

a = np.random.randint(1, 10, size=10)
print("将数组本身排序：")
a.sort()
print(a)

a = np.array([[1, 4, 3], [3, 1, 7], [8, 5, 10], [4, 2, 15]])
print("沿最后一个轴排序：")
print(np.sort(a))

b = np.sort(a, axis=None)
print("将数组所有数据后排序：")
print(b)

c = np.array([[1, 4, 5], [13, 1, 6], [18, 5, 9], [14, 2, 10]])
print("沿第一个轴排序：")
d = np.sort(c, axis=0)
print(d)


