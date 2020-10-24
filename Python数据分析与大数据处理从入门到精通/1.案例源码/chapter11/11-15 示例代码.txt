import pandas as pd

init_data = ["hello", "world", "python"]
data = pd.Series(init_data, index=[1, 2, 3])
print(data)
