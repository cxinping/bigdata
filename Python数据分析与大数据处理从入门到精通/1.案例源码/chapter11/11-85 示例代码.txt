import pandas as pd
import numpy as np

series1 = {"name": pd.Series(["Wilson", "Bruce", "Chelsea"]),
           "age": pd.Series([15, 24, 19]),
           "math_score": pd.Series([98, 88, 80]),
           "en_score": pd.Series([70, 68, 85]),
           "sum_score": pd.Series([168, 156, 165])}

df1 = pd.DataFrame(series1)
print("英语总成绩与平均成绩：", df1["en_score"].aggregate([np.sum, np.mean]))
print("数学总成绩与平均成绩：", df1["math_score"].aggregate([np.sum, np.mean]))


