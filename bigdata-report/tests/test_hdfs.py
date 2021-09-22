# -*- coding: utf-8 -*-

hdfs_file_url = 'hdfs://nameservice1/user/hive/warehouse/03_basal_layer_zfybxers00.db/zfybxers00_z_rmc_travel_allowance_m/importdate=20210921/000002_0'

# /user/hive/warehouse/03_basal_layer_zfybxers00.db
localDirUrl ='/my_filed_algos/prod_kudu_data/'
local_file = hdfs_file_url.replace('hdfs://nameservice1/', localDirUrl)
print(local_file)

# /my_filed_algos/prod_kudu_data/user/hive/warehouse/03_basal_layer_zfybxers00.db/BGM_QUOTA_DETAIL/importdate=20210909/20210909190518