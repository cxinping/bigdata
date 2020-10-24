import pandas as pd

series1 = {"name": pd.Series(["Wilson", "Bruce", "Chelsea"]),
           "age": pd.Series([15, 24, 19]),
           "math_score": pd.Series([98, 88, 80]),
           "en_score": pd.Series([70, 68, 85])}

df1 = pd.DataFrame(series1)
for item in df1.itertuples():
    print(item)


