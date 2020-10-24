import pandas as pd
import numpy as np

series1 = {"name": pd.Series(["Wilson", "Bruce", "Chelsea"]),
           "age": pd.Series([15, 24, 19]),
           "math_score": pd.Series([98, 88, 80]),
           "en_score": pd.Series([70, 68, 85])}

df1 = pd.DataFrame(series1)


def f(df, p1, p2):
    df["math_score"] = df["math_score"] + p1
    df["en_score"] = df["en_score"] + p2
    return df


df1 = df1.pipe(f, 10, 20)
print(df1)
