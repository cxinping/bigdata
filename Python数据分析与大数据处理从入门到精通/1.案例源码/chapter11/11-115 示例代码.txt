import pandas as pd

day = pd.date_range("2019-01-15", "2019-01-25", freq="D")
print("按天创建：")
print(day)
time = pd.date_range("01:00", "05:59", freq="H").time
print("按小时创建：")
print(time)


