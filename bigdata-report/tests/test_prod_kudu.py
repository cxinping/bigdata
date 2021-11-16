# -*- coding: utf-8 -*-
from report.commons.connect_kudu import prod_execute_sql

sql = """
describe analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets
"""

# select * from 01_datamart_layer_007_h_cw_df.finance_unusual
# describe analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets
# select * from analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets where unusual_id='33'

records = prod_execute_sql(conn_type='prod', sqltype='select', sql=sql)
for record in records:
    print(record)

