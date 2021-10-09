# -*- coding: utf-8 -*-

from report.commons.logging import get_logger
from report.commons.connect_kudu import prod_execute_sql, dis_connection
import time
import pandas as pd

log = get_logger(__name__)


def query_kudu_data(sql, columns):
    """
    发票日期异常检查
    :return:
    """
    records = prod_execute_sql(conn_type='test', sqltype='select', sql=sql)
    log.info('***' * 10)
    log.info('*** query_kudu_data=>' + str(len(records)))
    log.info('***' * 10)

    dataFromKUDU = []
    for item in records:
        record = []
        if columns:
            for idx in range(len(columns)):
                # print(item[idx], type(item[idx]))

                if str(item[idx]) == "None":
                    record.append(None)
                elif str(type(item[idx])) == "<java class 'JDouble'>":
                    record.append(float(item[idx]))
                else:
                    record.append(str(item[idx]))

        dataFromKUDU.append(record)

    df = pd.DataFrame(data=dataFromKUDU, columns=columns)
    return df


def check_49_data():
    columns_ls = ['finance_travel_id', 'bill_id', 'check_amount']
    columns_str = ",".join(columns_ls)

    sql = 'select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_official_bill limit 10 '.format(
        columns_str=columns_str)
    # count_sql = 'select count(a.finance_travel_id) from ({sql}) a'.format(sql=sql)
    # log.info(count_sql)
    # records = prod_execute_sql(conn_type='test', sqltype='select', sql=count_sql)
    # count_records = records[0][0]
    # log.info(f'* count_records ==> {count_records}')
    # records = prod_execute_sql(conn_type='test', sqltype='select', sql=sql)

    start_time = time.perf_counter()
    rd_df = query_kudu_data(sql, columns_ls)
    print(rd_df.head())
    print(rd_df.dtypes)
    print('*' * 50)
    print(rd_df.describe())

    temp = rd_df.describe()[['check_amount']]
    mean_val = temp.at['mean', 'check_amount']  # 平均值
    std_val = temp.at['std', 'check_amount']  # 方差

    result = rd_df[rd_df['check_amount'] > std_val]
    print(result)

    bill_id_ls = result['bill_id'].tolist()
    exec_sql(bill_id_ls)

    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'* 查询耗时 {consumed_time} sec')


def exec_sql(bill_id_ls):
    sql = """
    WHERE
    bill_id IN {bill_id_ls}
        """.format(bill_id_ls=tuple(bill_id_ls)).replace('\n', '').replace('\r', '').strip()
    print(sql)
    print(len(bill_id_ls))

    start_time = time.perf_counter()
    # prod_execute_sql(sqltype='insert', sql=sql)
    consumed_time = round(time.perf_counter() - start_time)
    log.info(f'*** 执行SQL耗时 {consumed_time} sec')


if __name__ == "__main__":
    check_49_data()
    print('--- ok ---')
