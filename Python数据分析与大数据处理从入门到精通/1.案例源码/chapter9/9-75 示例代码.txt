import numpy as np

a = [7, 6, 5, 4, 3, 10, 12, 15]
b = [9, 4, 0, 4, 0, 2, 1, 7]
ind = np.lexsort((b, a))
print("lexsort返回a各个元素在数组中的排序位置：")
print(ind)
d = [(a[i], b[i]) for i in ind]
print("通过列表推导式创建的新数组：")
print(d)


