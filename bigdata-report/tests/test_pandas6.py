import pandas as pd

def demo1():
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


def demo2():
    import pandas as pd
    import numpy as np
    df = pd.DataFrame({'Country': ['China', 'China', 'India', 'India', 'America', 'Japan', 'China', 'India'],
                       'Income': [10000, 10000, 5000, 5002, 40000, 50000, 8000, 5000],
                       'Age': [50, 43, 34, 40, 25, 25, 45, 32]})
    print(df)
    df_new = df.set_index('Age',  drop=True)
    print(df_new)

def demo3():
    import numpy as np
    from matplotlib import pyplot as plt

    x = np.arange(1, 11)
    y = 2 * x + 5
    plt.title("Matplotlib demo")
    plt.xlabel("x axis caption")
    plt.ylabel("y axis caption")
    plt.plot(x, y)
    plt.show()


if __name__ == '__main__':
    demo2()

    print('--- ok ---')