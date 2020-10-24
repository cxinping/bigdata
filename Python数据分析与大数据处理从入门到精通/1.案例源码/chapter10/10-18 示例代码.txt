# !/usr/bin/python
# -*- coding: UTF-8 -*-

import matplotlib.pyplot as plt

lines = plt.plot([1, 2, 3, 4], [1, 4, 9, 16])
plt.axis([0, 6, 0, 20])
lines[0].set_antialiased(False)
plt.title("线性图", fontproperties="SimHei")
plt.xlabel("x轴", fontsize=14, color="red", fontproperties="SimHei")
plt.ylabel("y轴", fontproperties="SimHei")
plt.text(2, 15, r"数据走势", color="green", fontproperties="SimHei")
plt.show()


