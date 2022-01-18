# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from pyramid.arima import auto_arima
from report.commons.logging import get_logger
import os
from report.commons.connect_kudu2 import prod_execute_sql
from report.commons.settings import CONN_TYPE
from report.commons.db_helper import query_kudu_data
import sys

sys.path.append('/you_filed_algos/app')

log = get_logger(__name__)

dest_dir = '/you_filed_algos/prod_kudu_data/temp'
dest_file = dest_dir + '/arima_data.txt'

test_limit_cond = ''  # 'LIMIT 1000'``


def init_file(dest_file):
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)

    if os.path.exists(dest_file):
        os.remove(dest_file)

    os.mknod(dest_file)


def query_data():
    columns_ls = ['bill_beg_date', 'between_date', 'member_cont']
    columns_str = ",".join(columns_ls)

    sql = f"""
        select 
    	bill_beg_date,
    	sum(between_date) as between_date, 
    	sum(member_cont)  as member_cont
    from  
    	01_datamart_layer_007_h_cw_df.finance_temporary_api 
    where 
    	temporary_number="12" and isCompany in ('gufen')
    and bill_beg_date >=substr(from_unixtime(unix_timestamp(to_date(months_add(concat('2021-12','-','01') , -12)),'yyyy-MM-dd'),'yyyyMMdd'),1,6)
    and bill_beg_date <substr(from_unixtime(unix_timestamp(to_date(months_add(concat('2021-12','-','01') , 0)),'yyyy-MM-dd'),'yyyyMMdd'),1,6) 
    group by bill_beg_date
    order by bill_beg_date desc
        """

    sql2 = f"""
            select 
        	bill_beg_date,
        	sum(between_date) as between_date, 
        	sum(member_cont)  as member_cont
        from  
        	01_datamart_layer_007_h_cw_df.finance_temporary_api 
        where 
        	temporary_number="12" and isCompany in ('gufen')    
        group by bill_beg_date
        order by bill_beg_date desc
            """

    count_sql = 'select count(1) from ({sql}) a'.format(sql=sql2)
    # log.info(count_sql)
    records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=count_sql)
    count_records = records[0][0]
    log.info(f'* count_records ==> {count_records}')

    df = query_kudu_data(sql=sql2, columns=columns_ls, )
    return df


def query_data2():
    init_file(dest_file)

    columns_ls = ['bill_beg_date', 'between_date', 'member_cont']
    columns_str = ",".join(columns_ls)

    sql = f"""
        select 
    	bill_beg_date,
    	sum(between_date) as between_date, 
    	sum(member_cont)  as member_cont
    from  
    	01_datamart_layer_007_h_cw_df.finance_temporary_api 
    where 
    	temporary_number="12" and isCompany in ('gufen')
    and bill_beg_date >=substr(from_unixtime(unix_timestamp(to_date(months_add(concat('2021-12','-','01') , -12)),'yyyy-MM-dd'),'yyyyMMdd'),1,6)
    and bill_beg_date <substr(from_unixtime(unix_timestamp(to_date(months_add(concat('2021-12','-','01') , 0)),'yyyy-MM-dd'),'yyyyMMdd'),1,6) 
    group by bill_beg_date
    order by bill_beg_date desc
        """

    sql2 = f"""
            select 
        	bill_beg_date,
        	sum(between_date) as between_date, 
        	sum(member_cont)  as member_cont
        from  
        	01_datamart_layer_007_h_cw_df.finance_temporary_api 
        where 
        	temporary_number="12" and isCompany in ('gufen')    
        group by bill_beg_date
        order by bill_beg_date desc
            """

    records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sql2)
    if records and len(records) > 0:
        result = []

        for idx, record in enumerate(records):
            bill_beg_date = str(record[0])
            between_date = int(record[1])
            member_cont = int(record[2])

            record_str = f'{bill_beg_date},{between_date},{member_cont}'
            result.append(record_str)

            if len(result) >= 100:
                for item in result:
                    with open(dest_file, "a+", encoding='utf-8') as file:
                        file.write(item + "\n")
                result = []

        if len(result) > 0:
            for item in result:
                with open(dest_file, "a+", encoding='utf-8') as file:
                    file.write(item + "\n")

        del result

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

    # 排序
    data = df.sort_values(axis=0, ascending=True, by=['bill_beg_date']).reset_index(drop=True)

    # 将日期作为索引
    data.set_index(['bill_beg_date'], inplace=True)

    # 将索引改为时间序列形式
    data.index = pd.to_datetime(data.index, format='%Y%m')

    # 将金额字段的格式改为float
    data['between_date'].values.astype(np.float)
    data['member_cont'].values.astype(np.float)

    between_date = data.drop(['member_cont'], axis=1)
    member_cont = data.drop(['between_date'], axis=1)
    print(type(between_date))
    print(between_date.head())

    # 设定样本和测试数据
    train_data = between_date[:int(0.7 * (len(between_date)))]
    test_data = between_date[int(0.7 * (len(between_date))):]

    print(len(between_date), len(train_data), len(test_data))
    print(type(train_data))

    # 自动判断序列的p/d/q，建立Arima预测模型
    model = auto_arima(train_data, trace=True, error_action='ignore', suppress_warnings=True)
    model.fit(train_data)

    # 预测数据
    forecast = model.predict(n_periods=len(test_data))
    forecast = pd.DataFrame(forecast, index=test_data.index, columns=['Prediction'])

    print(forecast)


if __name__ == '__main__':
    exec_arima()

    # query_data2()

    print('--- ok ---')
