# -*- coding: utf-8 -*-
import pandas as pd

dataFromHana = ['a', 'b', 'c']
df = pd.DataFrame(dataFromHana)
print(df)

id_list = [1, 2, 3]
id_list = ','.join([str(i) for i in id_list])
print(id_list)



