import pandas as pd

series1 = {"name": pd.Series(["Wilson", "Bruce", "Chelsea"]),
           "age": pd.Series([15, 24, 19]),
           "math_score": pd.Series([80, 88, 98])}

series2 = {"name": pd.Series(["Warren", "Leon", "Edith"]),
            "age": pd.Series([21, 18, 15]),
            "score": pd.Series([70, 68, 85])}

df1 = pd.DataFrame(series1)
df2 = pd.DataFrame(series2)
df1 = df1.reindex_like(df2)
print(df1)

