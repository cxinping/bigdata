import matplotlib.pyplot as plt
import numpy as np

data_x = np.array([1, 2, 3, 4, 5])
data_y = np.array([3, 5, 4, 7, 10])

plt.ylabel("数字序列", fontproperties="SimHei")

plt.plot(data_x * 2, data_y, 'r--', data_x * 3, data_y, 'bs')
plt.show()


