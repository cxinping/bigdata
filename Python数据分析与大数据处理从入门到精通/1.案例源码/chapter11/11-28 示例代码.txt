import pandas as pd

series = {"name": pd.Series(["Wilson", "Bruce", "Chelsea"], index=["user1", "user2", "user3"]),
          "age": pd.Series([15, 24, 19], index=["user1", "user2", "user3"]),
          "gender": pd.Series(["man", "man", "woman"], index=["user1", "user2", "user3"])}

df = pd.DataFrame(series)
print(df)
print("选取第1行数据：")
print(df.loc["user1"])
print("")
print("选取第2行数据：")
print(df.loc["user2"])
