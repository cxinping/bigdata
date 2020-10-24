import pandas as pd

series = {"name": pd.Series(["Wilson", "Bruce", "Chelsea"]),
          "age": pd.Series([15, 24, 19]),
          "gender": pd.Series(["man", "man", "woman"])}

df = pd.DataFrame(series)

print(df[0:2])
