# !/usr/bin/python
# -*- coding: UTF-8 -*-
import matplotlib.pyplot as plt
import numpy as np

plt.rcParams["font.sans-serif"] = ["SimHei"]

mean_scores = np.array([94, 85, 75, 91, 69, 83, 89, 86])
term = ["第一学习", "第二学习", "第三一学习", "第四学习",
        "第五学习", "第六学习", "第七学习", "第八学习"]

plt.plot(term, mean_scores, color="r", marker="o")

plt.xlabel("成绩分析")
plt.ylabel("成绩")
plt.show()


