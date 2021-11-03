# -*- coding: utf-8 -*-
import pandas as pd

df1 = pd.DataFrame({'a': [1, 2, 3], 'b': [1, 2, 3]})
df2 = pd.DataFrame({'a': [4, 5, 6], 'b': [4, 4, 4]})

res = pd.concat([df1, df2], axis=0, ignore_index=True)

print(res)

print('*' * 20)

import pandas as pd


def complex_function(x, y=0):
    print(f'x={x},y={y}')
    if x > 5 and x > y:
        return 1
    else:
        return 2


df = pd.DataFrame(data={'col1': [1, 4, 6, 2, 7], 'col2': [6, 7, 1, 2, 8]})
df['col3'] = df.apply(lambda x: complex_function(x['col1'], x['col2']), axis=1)

print(df)

print('*' * 100)

ipl_data = {'Team': ['Riders', 'Riders', 'Devils', 'Devils', 'Kings',
                     'kings', 'Kings', 'Kings', 'Riders', 'Royals', 'Royals', 'Riders'],
            'Rank': [1, 2, 2, 3, 3, 4, 1, 1, 2, 4, 1, 2],
            'Year': [2014, 2015, 2014, 2015, 2014, 2015, 2016, 2017, 2016, 2014, 2015, 2017],
            'Points': [876, 789, 863, 673, 741, 812, 756, 788, 694, 701, 804, 690]}

df = pd.DataFrame(ipl_data)
grouped = df.groupby('Year')

for name, group in grouped:
    print(name, type(name))
    print(group, type(group))
    print('\n')
