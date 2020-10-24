import numpy as np

a_matrix = np.array([[1, 1], [1, 1]])
b_matrix = np.array([[2, 0], [3, 4]])
print("同一位置元素相乘：")
print(a_matrix * b_matrix)

print("矩阵乘法：")
print(a_matrix @ b_matrix)

print("矩阵乘法：")
print(a_matrix.dot(b_matrix))


