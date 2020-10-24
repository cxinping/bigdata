# !/usr/bin/python
# -*- coding: UTF-8 -*-
import matplotlib.pyplot as plt

plt.rcParams["font.sans-serif"] = ["SimHei"]

leves = ["优", "良", "中", "差"]
data = [10, 15, 20, 15]
colors = ["red", "green", "purple", "royalblue"]
explode = [0.1, 0, 0, 0]
plt.pie(data, explode=explode, colors=colors,
        labels=leves, labeldistance=1.1,
        shadow=False, startangle=90, pctdistance=0.6)

plt.legend()
plt.show()




