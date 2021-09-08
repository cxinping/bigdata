#!/bin/sh
export PYTHON_EGG_CACHE=./myeggs
kinit -kt sjfw_admin.keytab sjfw_admin


impala-shell -k -i hadoop-pro-025 --ssl -b hadoop-pro-017 -q "

UPSERT into 01_datamart_layer_007_h_cw_df.finance_all_targets 
select a.bill_id, 
'03' as unusual_id,
a.company_code,
a.account_period,
a.account_item,
a.finance_number,
a.cost_center,
a.profit_center,
'' as cart_head,
a.bill_code,
a.origin_name   as  origin_city,
a.destin_name  as destin_city,
a.travel_beg_date  as beg_date,
a.travel_end_date  as end_date,
'' as emp_name,
'' as emp_code,
a.jour_amount,
a.accomm_amount,
a.subsidy_amount,
a.other_amount,
a.check_amount,
a.jzpz
from (
	select bill_id,company_code,account_period,
		account_item,finance_number,cost_center,
		profit_center,bill_code,origin_name,destin_name,
		travel_beg_date,travel_end_date,jour_amount,
		accomm_amount,subsidy_amount,other_amount,
		check_amount,jzpz from (
	select 
		bill_id,company_code,account_period,
		account_item,finance_number,cost_center,
		profit_center,bill_code,origin_name,destin_name,
		travel_beg_date,travel_end_date,jour_amount,
		accomm_amount,subsidy_amount,other_amount,
		check_amount,jzpz,
		sum(jzpz) as sum_jzpz_amount
	from 01_datamart_layer_007_h_cw_df.finance_travel_bill
	group by bill_id,company_code,account_period,
		account_item,finance_number,cost_center,
		profit_center,bill_code,origin_name,destin_name,
		travel_beg_date,travel_end_date,jour_amount,
		accomm_amount,subsidy_amount,other_amount,
		check_amount,jzpz  
	) y where  check_amount > sum_jzpz_amount 
)a


"







