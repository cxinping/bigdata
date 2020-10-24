import numpy as np

a = np.array([7, 6, 5, 4, 3, 10, 12, 15])
print("一维数组各元素求和：", np.sum(a))
print("一维数组求均值：", np.mean(a))
print("一维数组求最大值：", np.max(a))
print("一维数组求方差：", np.std(a))
print("一维数组求最大元素索引：", np.argmax(a))

a = np.array([7, 6, 5, 4, 3, 10, 12, 15]).reshape(4, 2)
print("二维数组各元素迭代求和：", np.cumsum(a))
print("二维数组将全部元素求和：", np.sum(a))
print("二维数组沿轴求和：", np.sum(a, axis=1))


