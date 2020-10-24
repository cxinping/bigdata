import pandas as pd

series = {"name": pd.Series(["Wilson", "Bruce", "Chelsea"]),
          "age": pd.Series([15, 24, 19]),
          "gender": pd.Series(["man", "man", "woman"]),
          "math_score": pd.Series([80, 88, 98]),
          "en_score": pd.Series([70, 68, 85])}

df = pd.DataFrame(series)
print(df)
print("索引重命名：")
print(df.rename(index={0: "第1行", 1: "第2行", 2: "第3行"},
                columns={"name": "姓名", "age": "年龄", "gender": "性别"
                    , "en_score": "英语", "math_score": "数学"}))
