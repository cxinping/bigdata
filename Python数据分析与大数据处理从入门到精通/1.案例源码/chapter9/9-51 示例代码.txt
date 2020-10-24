import numpy as np

a = np.arange(16)
b = a
if b is a:
    print("b和a是一样的")

print("a的地址：", id(a))
print("b的地址：", id(b))
b.shape = 4, 4
print("a的形状为：", a.shape)


