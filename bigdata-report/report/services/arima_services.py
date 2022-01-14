# -*- coding: utf-8 -*-

from gevent import monkey;

monkey.patch_all(thread=False)

import gevent
from gevent.pool import Pool
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


def exec_arima(query_date=None):
    #init_file(dest_file)

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
    count_sql = 'select count(1) from ({sql}) a'.format(sql=sql)
    log.info(count_sql)
    records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=count_sql)
    count_records = records[0][0]
    log.info(f'* count_records ==> {count_records}')

    df = query_kudu_data(sql=sql, columns=columns_ls,)
    log.info(df)


if __name__ == '__main__':
    exec_arima()

    print('--- ok ---')
