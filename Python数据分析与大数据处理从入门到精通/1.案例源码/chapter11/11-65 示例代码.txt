import pandas as pd
import numpy as np

series1 = {"name": pd.Series(["Wilson", "Bruce", "Chelsea"]),
           "age": pd.Series([15, 24, 19]),
           "math_score": pd.Series([98, 88, 80]),
           "en_score": pd.Series([70, 68, 85])}

df1 = pd.DataFrame(series1)


def f(item):
    if isinstance(item, int):
        return item + 10
    elif isinstance(item, str):
        return "hello " + item


df1 = df1.applymap(f)
print(df1)
