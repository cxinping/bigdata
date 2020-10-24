import pandas as pd

series = {"name": pd.Series(["Wilson", "Bruce", "Chelsea"]),
          "age": pd.Series([15, 24, 19]),
          "gender": pd.Series(["man", "man", "woman"]),
          "math_score": pd.Series([80, 88, 98]),
          "en_score": pd.Series([70, 68, 85])}

df = pd.DataFrame(series)
print(df)
print("重建索引：")
print(df.reindex(index=[0, 1, 3],
                 columns={"name", "age", "height"}))
