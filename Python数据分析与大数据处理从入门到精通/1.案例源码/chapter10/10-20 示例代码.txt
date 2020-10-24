# !/usr/bin/python
# -*- coding: UTF-8 -*-

import matplotlib.pyplot as plt

plt.plot([1, 5, 10, 15, 20, 25], [1, 5, 10, 15, 20, 25])

plt.annotate("中间值", xy=(12.5, 12.5), xytext=(15, 10), fontproperties="SimHei",
             arrowprops=dict(facecolor='black', shrink=0.05))
plt.show()


