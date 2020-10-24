import numpy as np

dt = np.linspace(20, 30, num=5)
print("第一个数组：")
print(dt)

print("第二个数组：")
dt = np.linspace(20, 30, num=5, endpoint=False)
print(dt)

print("第三个数组：")
dt = np.linspace(20, 30, num=5, retstep=True)
print(dt)


