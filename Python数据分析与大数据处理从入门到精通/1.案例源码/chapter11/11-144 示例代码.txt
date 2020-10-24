import pandas as pd

series = {"Wilson": pd.Series([98, 92]),
          "Bruce": pd.Series([88, 68]),
          "Chelsea": pd.Series([91, 94]),
          "Warren": pd.Series([94, 78]),
          "Ivy": pd.Series([74, 69])}

df1 = pd.DataFrame(series)
df2 = df1.rename(index={0: "math_score", 1: "en_score"})
print("原始数据：")
print(df2)
print("筛选后的数据：")
print(df2.loc[:, (df2.iloc[0] > 95) & (df2.iloc[1] > 90)])


