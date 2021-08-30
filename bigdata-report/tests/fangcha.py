# -*- coding: utf-8 -*-
import numpy as np
arr = [1,2,3,4,5,6]
#求均值
arr_mean = np.mean(arr)
#求方差
arr_var = np.var(arr)
#求标准差
arr_std = np.std(arr,ddof=1)
print("平均值为：%f" % arr_mean)
print("方差为：%f" % arr_var)
print("标准差为:%f" % arr_std)

print('-------' * 20)
import pandas as pd
df = pd.DataFrame(np.array([[85, 68, 90], [82, 63, 88], [84, 90, 78]]), columns=['统计学', '高数', '英语'], index=['张三', '李四', '王五'])

print(df)
print('\n')
# 显示每一列的平均数
print(df.mean())

print('\n')
# 显示每一行的平均数
print( df.mean(axis = 1))
print('\n')
# 显示每一列的方差
df_var = df.var()
print( df_var  )
print(type( df_var ))
print(df_var.tolist() )


