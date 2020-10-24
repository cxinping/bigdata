import numpy as np

a = np.arange(10)
print(a)
print("通过下标选择元素：", a[5])
print("通过切片选择元素：", a[3:8])
print("通过切片设置步长选择元素：", a[::2])
print("循环数组：")
for item in a:
    print("当前元素是：", item * 5)

	
	