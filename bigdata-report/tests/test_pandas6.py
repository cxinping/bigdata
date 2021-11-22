import pandas as pd

salaries = pd.DataFrame({
    'name': ['BOSS', 'Lilei', 'Lilei', 'Han', 'BOSS', 'BOSS', 'Han', 'BOSS'],
    'Year': [2016, 2016, 2016, 2016, 2017, 2017, 2017, 2017],
    'Salary': [1, 2, 3, 4, 5, 6, 7, 8],
    'Bonus': [2, 2, 2, 2, 3, 4, 5, 6]
})
print(salaries)
# print(salaries['Bonus'].duplicated(keep='first'))
# print(salaries[salaries['Bonus'].duplicated(keep='first')].index)
# print(salaries[salaries['Bonus'].duplicated(keep='first')])
# print(salaries['Bonus'].duplicated(keep='last'))
# print(salaries[salaries['Bonus'].duplicated(keep='last')].index)
# print(salaries[salaries['Bonus'].duplicated(keep='last')])

print('=' * 30)

print(salaries[salaries.duplicated('Bonus', keep=False) == False])

print('=' * 30)

# print(salaries['Bonus'].duplicated(keep=False))
