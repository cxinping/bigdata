#!/bin/sh
export PYTHON_EGG_CACHE=./myeggs
kinit -kt sjfw_admin.keytab sjfw_admin


impala-shell -k -i hadoop-pro-025 --ssl -b hadoop-pro-017 -q "

UPSERT into 01_datamart_layer_007_h_cw_df.finance_all_targets 
SELECT bill_id, 
'10' as unusual_id,
company_code,
account_period,
account_item,
finance_number,
cost_center,
profit_center,
'' as cart_head,
bill_code,
origin_name   as  origin_city,
destin_name  as destin_city,
travel_beg_date  as beg_date,
travel_end_date  as end_date,
'' as emp_name,
'' as emp_code,
jour_amount,
accomm_amount,
subsidy_amount,
other_amount,
check_amount,
jzpz
FROM 01_datamart_layer_007_h_cw_df.finance_travel_bill 
WHERE apply_id='' or unix_timestamp(base_apply_date, 'yyyyMMdd') > unix_timestamp(jour_beg_date, 'yyyyMMdd')
group by bill_id,company_code,account_period,account_item,finance_number,cost_center,profit_center,bill_code,origin_name,
destin_name,travel_beg_date,travel_end_date,jour_amount,accomm_amount,subsidy_amount,other_amount,check_amount,jzpz

"







