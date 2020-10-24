# !/usr/bin/python
# -*- coding: UTF-8 -*-

import matplotlib.pyplot as plt

plt.figure(1)
plt.subplot(211)
plt.plot([1, 2, 3])
plt.subplot(212)
line = plt.plot([4, 5, 6])

plt.figure(2)
plt.plot([4, 5, 6])
plt.show()

plt.figure(1)
plt.subplot(212)
plt.title("第1张画图的第2个子图", fontproperties="SimHei")
plt.show()


