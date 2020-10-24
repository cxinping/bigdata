import pandas as pd

series = {"name": pd.Series(["Wilson", "Bruce", "Chelsea"], index=["user1", "user2", "user2"]),
          "age": pd.Series([15, 24, 19], index=["user1", "user2", "user2"]),
          "gender": pd.Series(["man", "man", "woman"], index=["user1", "user2", "user2"])}

df = pd.DataFrame(series)

print(df.drop("user2"))
