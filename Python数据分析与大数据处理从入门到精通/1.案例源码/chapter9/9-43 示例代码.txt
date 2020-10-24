import numpy as np

a = np.floor(10 * np.random.random((4, 5)))
print("修改前形状为：", a.shape)
a.resize(2, 10)
print("修改后形状为：", a.shape)


