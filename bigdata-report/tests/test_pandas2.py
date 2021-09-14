# -*- coding: utf-8 -*-
import pandas as pd

pd.set_option('display.max_columns', None)   # 显示完整的列
pd.set_option('display.max_rows', None)  # 显示完整的行
pd.set_option('display.expand_frame_repr', False)  # 设置不折叠数据

# 假设有 5 个人，分别参加了 4 门课程，获得了对应的分数
# 同时这个 5 个人分别负责的项目个数 在 'Project_num' 列中显示
data = {'name' : pd.Series(['Alice', 'Bob', 'Cathy', 'Dany', 'Ella', 'Ford', 'Gary', 'Ham', 'Ico', 'Jack']),
        'Math_A' : pd.Series([1, 2, 1, 2, 1, 2, 3, 1, 2, 1]),
        'English_A' : pd.Series([3, 2.6, 2, 1.7, 3, 3.3, 4.4, 5, 3.2, 2.4]),
        'Math_B' : pd.Series([1.7, 2.5, 3.6, 2.4, 5, 2.2, 3.3, 4.4, 1.5, 4.3]),
        'English_B' : pd.Series([5, 2.6, 2.4, 1.3, 3, 3.6, 2.4, 5, 2.2, 3.1]),
        'Project_num' : pd.Series([2, 3, 0, 1, 7, 2, 1, 5, 3, 4]),
        'Sex' : pd.Series(['F', 'M', 'M', 'F', 'M', 'F', 'M', 'M', 'F', 'M'])
     }
df = pd.DataFrame(data)
print(df)

#  整体统计描述 df.describe()
# 中位数，平均值
print(df.describe())

print(' ' * 10 )
# 1.4.3 对指定的列
print(df.Math_A.describe())

print(' ' * 10 )
# 计算标准方差
print(type(df.std()),df.std())

print(' ' * 10 )

std = df.std()
json_obj = std.to_json(orient='split')
print(type(json_obj), json_obj)

idex_ls =[]
value_ls = []
for i, v in std.items():
   print('index=', i, ',value=', v)
   idex_ls.append(i)
   value_ls.append(v)

data = {'index' : idex_ls , 'value' : value_ls}
print(data)

import json
json_str = json.dumps(data)
print(json_str)