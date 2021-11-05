# -*- coding: utf-8 -*-
import pandas as pd

data = {"one": [2580, 2580, 2580, 2580, 2600, 2600]}
rd_df = pd.DataFrame(data)

print(rd_df.describe())

temp = rd_df.describe()[['one']]
std_val = temp.at['std', 'one']  # 方差
mean_val =  temp.at['mean', 'one']  # 平均值

print(std_val, mean_val)

# 数据的正常范围为 【mean-2 × std，mean+2 × std】
max_val = mean_val + 2 * std_val
min_val = mean_val - 2 * std_val
print(max_val, min_val)

# 2617.650533436326 2555.682799897007