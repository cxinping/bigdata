import numpy as np
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt
import six
import sys

sys.modules['sklearn.externals.six'] = six
import joblib

sys.modules['sklearn.externals.joblib'] = joblib
from pyramid.arima import auto_arima

import warnings

print('--- ok ---')


def query_data2():
    dest_file = 'D:/test5/arima_data.txt'
    rd_df = pd.read_csv(dest_file, sep=",", header=None,
                        names=["bill_beg_date", "between_date", "member_cont"])
    # print(rd_df)

    return rd_df


def exec_arima(query_date=None):
    # init_file(dest_file)
    df = query_data2()
    # log.info(df.info())

    # 过滤 between_date 的数据小于和等于0的
    df = df[df.apply(lambda x: x['between_date'] > 0, axis=1)]

    # df['bill_beg_date'] = df['bill_beg_date'].apply(lambda x: datetime.strptime(str(x), '%Y%m').date())

    # 排序
    data = df.sort_values(axis=0, ascending=True, by=['bill_beg_date']).reset_index(drop=True)

    # 将日期作为索引
    data.set_index(['bill_beg_date'], inplace=True)

    # 将索引改为时间序列形式
    data.index = pd.to_datetime(data.index, format='%Y%m')

    # 将金额字段的格式改为float
    data['between_date'].values.astype(np.float)
    data['member_cont'].values.astype(np.float)

    # print('***  使用的数据集 ********')
    # print(data)

    between_date = data.drop(['member_cont'], axis=1)
    member_cont = data.drop(['between_date'], axis=1)
    # print(type(between_date))
    print(between_date.head())

    # 设定样本和测试数据
    train_data = between_date[:int(0.7 * (len(between_date)))]
    test_data = between_date[int(0.7 * (len(between_date))):]

    print('***  测试的数据集 ********')
    print(test_data)

    # 看预测样本的曲线图
    # plt.plot(train_data)
    # plt.plot(test_data, label='Valid')
    # plt.show()

    print(len(between_date), len(train_data), len(test_data))
    # print(type(train_data))
    # print(train_data.info() )

    # 自动判断序列的p/d/q，建立Arima预测模型
    model = auto_arima(train_data, trace=True, error_action='ignore', suppress_warnings=True, seasonal=True, m=12)
    model.fit(train_data)

    dt_index = pd.to_datetime(['2022-01', '2022-02', '2022-03', '2022-04', '2022-05', '2022-06', '2022-07', '2022-08', '2022-09', '2022-10', '2022-11', '2022-12'])
    test_data2 = pd.DataFrame({'Prediction': [0, 0, 0, 0, 0, 0,0, 0, 0, 0, 0, 0]}, index=dt_index)
    # 预测数据
    forecast = model.predict(n_periods=len(test_data2))
    forecast = pd.DataFrame(forecast, index=test_data2.index, columns=['Prediction'])

    print('***  显示预测值 ********')
    print(len(forecast))
    print(forecast)

    # 展示预测曲线
    plt.plot(train_data)
    plt.plot(test_data, label='Valid')
    plt.plot(forecast, label='Prediction')
    plt.show()


if __name__ == '__main__':
    exec_arima()

    # query_data2()

    print('--- ok ---')
