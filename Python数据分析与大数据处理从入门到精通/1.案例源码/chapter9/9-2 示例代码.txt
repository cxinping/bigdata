import numpy as np

a = np.arange(20).reshape(4, 5)
print("创建一个4行5列的数组：")
print(a)
print("数组的轴数（维度）：", a.ndim)
print("数组的形状：", a.shape)
print("数组类型：", a.dtype.name)
print("数组元素的大小：", a.itemsize)
print("数组大小：", a.size)
print("数组a类型：", type(a))
b = np.array([6, 7, 8])
print("数组b类型：", type(b))


