
Pandas学习资料

> https://geek-docs.com/pandas/pandas-tutorials/pandas-dataframe-read-add-delete.html

## 轮询Pandas
方法1：下标循环

df1 = df
for i in range(len(df)):
    if df.iloc[i]['test'] != 1:
        df1.iloc[i]['test'] = 0
 
下标循环是通过循环一个下标数列，通过iloc去不断get数据，这个方法是新手最常用的但也是最慢的


方法2：Iterrows循环 

i = 0
for ind, row in df.iterrows():
    if row['test'] != 1:
        df1.iloc[i]['test'] = 0
    i += 1
该循环方式是通过iterrows进行循环，ind和row分别代表了每一行的index和内容。测试例子大概需要0.07s，比起下标循环速度提升了321倍。


方法3：Apply循环
df1['test'] = df['test'].apply(lambda x: x if x == 1 else 0)
Apply是pandas的一个常用函数，通常的用法是内接一个lambda匿名函数，从而对dataframe的每一行都进行循环处理。在测试例子中，apply的速度为0.027s，比下标循环快了811倍。

方法4：Pandas内置向量化函数

res = df.sum()
Pandas为我们提供了大量的内置向量化函数，比如sum，mean就可以快速计算某一列的求和和平均


## pycharm 控制台输出显示 pandas解决方案

import pandas as pd
pd.set_option('display.max_columns', a)  # 设置显示的最大列数参数为a
pd.set_option('display.max_rows', b)  # 设置显示的最大的行数参数为b
pd.set_option('display.width', 2000)






方法5：Numpy向量化函数


df_values = df.values
res = np.sum(df_values)
最后一种方法是将Pandas的数据转化为Numpy的Array，然后使用Numpy的内置函数进行向量化操作。

