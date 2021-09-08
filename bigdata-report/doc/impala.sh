#!/bin/sh
export PYTHON_EGG_CACHE=./myeggs
kinit -kt sjfw_admin.keytab sjfw_admin

importdate=$(date -d "1 day ago" +%Y%m%d)

impala-shell -k -i hadoop-pro-025 --ssl -b hadoop-pro-017 -q "

use 01_datamart_layer_007_h_cw_df;

refresh 01_datamart_layer_007_h_cw_df.hdberpoutputinvoiceinfo;


UPSERT into hdberpoutputinvoiceinfo SELECT * from 02_logical_layer_007_h_lf_cw.hdberpoutputinvoiceinfo   where importdate='${importdate}' ;

"
