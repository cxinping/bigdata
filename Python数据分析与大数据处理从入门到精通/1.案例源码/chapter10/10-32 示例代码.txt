# !/usr/bin/python
# -*- coding: UTF-8 -*-
import matplotlib.pyplot as plt

plt.rcParams["font.sans-serif"] = ["SimHei"]

names = ["Wilson", "Warren", "Leon", "Bruce", "Andrew", "Edith"]
score = [69, 85, 98, 71, 82, 99]

plt.barh(range(6), score, color="red")
plt.yticks(range(6), names)

for i in range(6):
    plt.text(score[i], i, score[i])

plt.show()


