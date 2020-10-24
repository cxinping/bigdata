# !/usr/bin/python
# -*- coding: UTF-8 -*-

import matplotlib.pyplot as plt
import seaborn as sns

sns.set()
fmri = sns.load_dataset("fmri")
sns.relplot(x="timepoint", y="signal", hue="subject",
            col="region", row="event", height=3,
            kind="line", estimator=None, data=fmri)
plt.show()

