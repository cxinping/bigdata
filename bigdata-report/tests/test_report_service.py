# -*- coding: utf-8 -*-
import time
import unittest

from report.commons.connect_kudu import dis_connection, prod_execute_sql
from report.services.travel_expense_service import getKUDUdata, check_10_beforeapply_amount, query_kudu_data, \
    check_03_consistent_amount


class ReportServiceTest(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_get_KUDUdata(self):
        print('--- test_get_KUDUdata ---')
        rd_df = getKUDUdata(
            'select apply_id, apply_code from 03_basal_layer_zfybxers00.ZFYBXERS00_Z_RMA_BASE_APPLY limit 30')

        print(rd_df)
        #print(rd_df.shape)

    def test_check_03_consistent_amount(self):
        check_03_consistent_amount()


    def test_check_10_beforeapply_amount(self):
        check_10_beforeapply_amount()

    def test_save_consistent_amount(self):
        """
        保存检查是否存在费用报销金额大于原始票据金额情况的结果到落地表
        :return:
        """
        sql = 'insert into 01_datamart_layer_007_h_cw_df.finance_03_checkamount values("{finance_travel_id}", "{bill_id}", "{member_emp_id}", "{apply_emp_name}", {check_amount} , {jzpz}, "{unusual_id}") '.format(
            finance_travel_id='013', bill_id='002', member_emp_id='003', apply_emp_name='004', check_amount=100,
            jzpz=200,
            unusual_id='03'
        )
        print(sql)
        prod_execute_sql(sqltype='insert', sql=sql)

    def test_test1(self):
        start_time = time.perf_counter()

        # select apply_id, apply_code from 03_basal_layer_zfybxers00.ZFYBXERS00_Z_RMA_BASE_APPLY limit 20
        # select finance_travel_id  from 01_datamart_layer_007_h_cw_df.finance_travel_bill
        columns = ['bill_id', 'apply_emp_name', 'prof_cent_sname', 'accomm_amount']
        sql = 'select bill_id, apply_emp_name, prof_cent_sname, accomm_amount  from 01_datamart_layer_007_h_cw_df.finance_travel_bill '
        rd_df = query_kudu_data(sql=sql, columns=columns)

        # print(rd_df.head(5))
        print(rd_df.shape)
        # print(rd_df.dtypes )

        # 取空
        rd_df = rd_df.dropna(axis=0, subset=['accomm_amount'])
        # 转换数据类型
        rd_df['accomm_amount'] = rd_df['accomm_amount'].astype('float')
        print(rd_df.head(5))
        print(rd_df.shape)
        print(rd_df.dtypes)

        # print(rd_df.iloc[0,0] , type(rd_df.iloc[0,0]))
        consumed_time = round(time.perf_counter() - start_time)
        print(f'* consumed_time={consumed_time} sec')
        dis_connection()


    def test_save_10_beforeapply_amount(self):
        """
        :return:
        """
        sql = 'insert into 01_datamart_layer_007_h_cw_df.finance_10_beforeapply values("{finance_travel_id}", "{bill_apply_id}", "{base_apply_id}", "{apply_date}", "{journey_date}" ,"{unusual_id}") '.format(
            finance_travel_id='013', bill_apply_id='002', base_apply_id='003', apply_date='004', journey_date='100',
            unusual_id='10'
        )

        sql2 = 'insert into 01_datamart_layer_007_h_cw_df.finance_10_beforeapply values("102", "02D3C06EFA3F02B0E0530AF680F15853", "00F8484AA80803BEE0530AF680F1D125", "20140909", "20140618" ,"10")'
        sql3 = """
    
        """


        prod_execute_sql(sqltype='insert', sql=sql2)
        dis_connection()










