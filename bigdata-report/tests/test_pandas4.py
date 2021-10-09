# -*- coding: utf-8 -*-
import pandas as pd

df1 = pd.DataFrame({'a': [1, 2, 3], 'b': [1, 2, 3]})
df2 = pd.DataFrame({'a': [4, 5, 6], 'b': [4, 4, 4]})

res = pd.concat([df1,df2],axis=0,ignore_index=True)

print(res)
