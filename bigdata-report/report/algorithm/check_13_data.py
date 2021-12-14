# -*- coding: utf-8 -*-

from gevent import monkey;
monkey.patch_all(thread=False)

import gevent
from gevent.pool import Pool

from report.commons.connect_kudu2 import prod_execute_sql
from report.commons.logging import get_logger
import time
import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
import threading
from report.services.common_services import ProvinceService
from report.commons.tools import list_of_groups
from report.services.common_services import query_billds_finance_all_targets
from report.commons.settings import CONN_TYPE

log = get_logger(__name__)

"""
异常值：一组测定值中与平均值的偏差超过两倍标准差的测定值

计算住宿费用的异常值

cd /you_filed_algos/app

PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/report/algorithm/check_13_data.py

"""

import sys

sys.path.append("/you_filed_algos/app")

dest_dir = "/you_filed_algos/prod_kudu_data/checkpoint13"
dest_file = dest_dir + "/check_13_data.txt"

test_limit_cond = ""  # " LIMIT 10002"


class Check13Service:

    def __init__(self):
        self.province_service = ProvinceService()

    def init_file(self):
        if not os.path.exists(dest_dir):
            os.makedirs(dest_dir)

        if os.path.exists(dest_file):
            os.remove(dest_file)

        os.mknod(dest_file)

    def save_fee_data(self):
        """
        查询超过标准报销费用的住宿费记录
        :return:
        """
        self.init_file()

        sql = f"""
        SELECT a.bill_id, a.city_name, a.city_grade_name, a.emp_name, a.stand_amount_perday, a.hotel_amount_perday
            FROM (
            SELECT DISTINCT
            bill_id, city_name, city_grade_name, emp_name,ROUND(stand_amount, 2) as stand_amount_perday,
            ROUND(hotel_amount/hotel_num , 2) as hotel_amount_perday
            FROM 01_datamart_layer_007_h_cw_df.finance_rma_travel_accomm
            WHERE exp_type_name="差旅费" AND hotel_num > 0
            AND bill_id is not null AND bill_id != ""           
            )as a   {test_limit_cond}        
        """

        count_sql = "select count(b.bill_id) from ({sql}) b".format(sql=sql)
        log.info(count_sql)
        records = prod_execute_sql(conn_type=CONN_TYPE, sqltype="select", sql=count_sql)
        count_records = records[0][0]
        print(f"* count_records ==> {count_records}")

        max_size = 10 * 10000
        limit_size = 10000
        select_sql_ls = []
        if count_records >= max_size:
            offset_size = 0
            while offset_size <= count_records:
                if offset_size + limit_size > count_records:
                    limit_size = count_records - offset_size
                    tmp_sql = """
                        SELECT a.bill_id, a.city_name, a.city_grade_name, a.emp_name,a.stand_amount_perday, a.hotel_amount_perday
                            FROM (
                            SELECT DISTINCT
                            bill_id, city_name, city_grade_name, emp_name,ROUND(stand_amount, 2) as stand_amount_perday,
                            ROUND(hotel_amount/hotel_num , 2) as hotel_amount_perday
                            FROM 01_datamart_layer_007_h_cw_df.finance_rma_travel_accomm
                            WHERE exp_type_name="差旅费" AND hotel_num > 0
                            AND bill_id is not null AND bill_id != ""
                            )as a                       
                        order by stand_amount_perday limit {limit_size} offset {offset_size}
                    """.format(limit_size=limit_size, offset_size=offset_size)

                    select_sql_ls.append(tmp_sql)
                    break
                else:
                    tmp_sql = """
                           SELECT a.bill_id, a.city_name, a.city_grade_name, a.emp_name,a.stand_amount_perday, a.hotel_amount_perday
                               FROM (
                               SELECT DISTINCT
                               bill_id, city_name, city_grade_name, emp_name,ROUND(stand_amount, 2) as stand_amount_perday,
                               ROUND(hotel_amount/hotel_num , 2) as hotel_amount_perday
                               FROM 01_datamart_layer_007_h_cw_df.finance_rma_travel_accomm
                               WHERE exp_type_name="差旅费" AND hotel_num > 0
                               AND bill_id is not null AND bill_id != ""
                               )as a                           
                           order by stand_amount_perday limit {limit_size} offset {offset_size}
                           """.format(limit_size=limit_size, offset_size=offset_size)

                    select_sql_ls.append(tmp_sql)

                offset_size = offset_size + limit_size
        else:
            tmp_sql = f"""           
        SELECT a.bill_id, a.city_name, a.city_grade_name, a.emp_name,a.stand_amount_perday, a.hotel_amount_perday
            FROM (
            SELECT DISTINCT
            bill_id, city_name, city_grade_name, emp_name,ROUND(stand_amount, 2) as stand_amount_perday,
            ROUND(hotel_amount/hotel_num , 2) as hotel_amount_perday
            FROM 01_datamart_layer_007_h_cw_df.finance_rma_travel_accomm  
            WHERE exp_type_name="差旅费" AND hotel_num > 0     
            AND bill_id is not null AND bill_id != ""        
            )as a
        WHERE a.hotel_amount_perday  > a.stand_amount_perday  {test_limit_cond}   
        """
            select_sql_ls.append(tmp_sql)

        log.info(f"* 开始分页查询，一共 {len(select_sql_ls)} 页")
        start_time = time.perf_counter()
        # threadPool = ThreadPoolExecutor(max_workers=10, thread_name_prefix="thr")
        # all_task = [threadPool.submit(self.exec_task, (sel_sql)) for sel_sql in select_sql_ls]
        # wait(all_task, return_when=ALL_COMPLETED)
        # threadPool.shutdown(wait=True)

        pool = Pool(30)
        results = []
        for sel_sql in select_sql_ls:
            rst = pool.spawn(self.exec_task, sel_sql)
            results.append(rst)

        gevent.joinall(results)

        consumed_time = round(time.perf_counter() - start_time)
        log.info(f"* 一共有数据 {count_records} 条，保存数据耗时 {consumed_time} sec")

    def exec_task(self, sql):
        # log.info(sql)

        records = prod_execute_sql(conn_type=CONN_TYPE, sqltype="select", sql=sql)

        if records and len(records) > 0:
            for idx, record in enumerate(records):
                bill_id = str(record[0])  # bill_id
                city_name = str(record[1])  # 出差城市名称
                city_grade_name = str(record[2])  # 出差城市等级
                emp_name = str(record[3])  # 员工名字
                stand_amount_perday = float(record[4])  # 每天标准住宿费用
                hotel_amount_perday = float(record[5])  # 每天实际花费的住宿费用

                province = self.province_service.query_belong_province(city_name)  # 出差城市所属的省
                # province = province if province else "******"   # 可能需要补充  01_datamart_layer_007_h_cw_df.finance_province_city 表中的数据

                city_name = city_name.replace(",", " ")
                emp_name = emp_name.replace(",", " ")

                record_str = f"{bill_id},{city_name},{province},{city_grade_name},{emp_name},{stand_amount_perday},{hotel_amount_perday}"
                # log.info(f"checkpoint_13 {threading.current_thread().name} is running ")
                # log.info(record_str)

                with open(dest_file, "a+", encoding="utf-8") as file:
                    file.write(record_str + "\n")

    def cal_abnormal_data(self, rd_df, coefficient, query_province):
        # query_province = "江苏省"
        abnormal_rd_df1 = rd_df[(rd_df["province"] == query_province)
                                & (rd_df["stand_amount_perday"] >= rd_df["hotel_amount_perday"])
                                & (rd_df["consume_amount_perday"] > 0)
                                ]
        # print("part1 after filter abnormal_rd_df1 ", len(abnormal_rd_df1))
        # print(abnormal_rd_df1.head(10))

        # part1 过滤查询
        # temp = abnormal_rd_df1.describe()[["consume_amount_perday"]]
        # std_val = temp.at["std", "consume_amount_perday"]  # 标准差
        # mean_val = temp.at["mean", "consume_amount_perday"]  # 平均值

        std_val = abnormal_rd_df1.std().at['consume_amount_perday'] # 标准差
        mean_val = abnormal_rd_df1.mean().at['consume_amount_perday'] # 平均值

        # 数据的正常范围为 【mean - 2 × std , mean + 2 × std】
        max_val = mean_val + coefficient * std_val
        min_val = mean_val - coefficient * std_val

        # print(f"{query_province} 方差 => {std_val}, 平均值 => {mean_val}, max_val => {max_val}, min_val => {min_val}")

        bill_id_ls1 = []
        for index, row in abnormal_rd_df1.iterrows():

            hotel_amount_perday = row["hotel_amount_perday"]
            # print("hotel_amount_perday=", hotel_amount_perday)

            if hotel_amount_perday > max_val or hotel_amount_perday < min_val:
                bill_id = row["bill_id"]
                bill_id_ls1.append(bill_id)

        # part2 过滤查询
        abnormal_rd_df2 = rd_df[(rd_df["province"] == query_province) & (
                rd_df["stand_amount_perday"] < rd_df["hotel_amount_perday"])]
        bill_id_ls2 = abnormal_rd_df2["bill_id"].tolist()
        bill_id_ls1.extend(bill_id_ls2)

        log.info(f"checkpoint_13 {query_province} 异常数量={len(bill_id_ls1)}")
        return bill_id_ls1

    def analyze_data(self, coefficient=2):
        log.info(f"* 开始执行检查点13 coefficient={coefficient} ")
        start_time = time.perf_counter()

        rd_df = pd.read_csv(dest_file, sep=",", header=None,
                            names=["bill_id", "city_name", "province", "city_grade_name", "emp_name",
                                   "stand_amount_perday", "hotel_amount_perday"])
        rd_df["consume_amount_perday"] = rd_df["stand_amount_perday"] - rd_df["hotel_amount_perday"]
        # print(rd_df.dtypes)

        province_names = self.province_service.query_province_names(grade="1")
        # print(province_names)
        bill_id_ls = []
        for idx, province_name in enumerate(province_names):
            # print(province_name)
            tmp_ls = self.cal_abnormal_data(rd_df=rd_df, coefficient=coefficient, query_province=province_name)
            # bill_id_ls.extend(tmp_ls)

            exec_sql(tmp_ls)

        log.info(f"* all results => {len(bill_id_ls)}")
        # targes_bill_id_ls = query_billds_finance_all_targets(unusual_id="13")
        # bill_id_ls = [x for x in bill_id_ls if x not in targes_bill_id_ls]
        # exec_sql(bill_id_ls)

        consumed_time = round(time.perf_counter() - start_time)
        log.info(f"* 执行检查点13的数据共耗时 {consumed_time} sec")


def exec_sql(bill_id_ls):
    log.info(f"checkpoint_13 exec_sql ==> {len(bill_id_ls)}")

    if bill_id_ls and len(bill_id_ls) > 0:
        group_ls = list_of_groups(bill_id_ls, 1000)
        # print(len(group_ls), group_ls)

        condition_sql = ""
        in_codition = "bill_id IN {temp}"

        for idx, group in enumerate(group_ls):
            if len(group) == 1:
                temp = in_codition.format(temp=str("(\"" + group[0] + "\")"))
            else:
                temp = in_codition.format(temp=str(tuple(group)))

            if idx == 0:
                condition_sql = temp
            else:
                condition_sql = condition_sql + " OR " + temp

        # print(condition_sql)

        sql = """        
        UPSERT INTO analytic_layer_zbyy_cwyy_014_cwzbbg.finance_all_targets    
        SELECT
        hotel_bill_id as finance_id,
        bill_id,
        "13" as unusual_id,
        "" as company_code,
        "" as account_period,
        "" as finance_number,
        "" as profit_center,
        "" as cart_head,
        "" as bill_code,
        "" as bill_beg_date,
        "" as bill_end_date,
        "" as origin_city,
        "" as destin_city,
        beg_date,
        end_date,
        "" as apply_emp_name,
        emp_name,
        emp_code,
        "" as company_name,
        0 as jour_amount,
        0 as accomm_amount,
        0 as subsidy_amount,
        0 as other_amount,
        0 as check_amount,
        0 as jzpz,
        "差旅费" as target_classify,
        0 as meeting_amount,
        exp_type_name,
        "" as next_bill_id,
        "" as last_bill_id,
        "" as appr_org_sfname,
        "" as sales_address,
        "" as meet_addr,
        "" as sponsor,
        0 as jzpz_tax,
        "" as billingdate,
        "" as remarks,
        hotel_amount,
        total_amount,
        "" as apply_id,
        "" as base_apply_date,
        "" as scenery_name_details,
        "" as meet_num,
        0 as diff_met_date,
        0 as diff_met_date_avg,
        "" as tb_times,
        "" as receipt_city,
        "" as commodityname,
        "" as category_name,
        "" as iscompany,
        "" as origin_province,
        "" as destin_province,
        "" as operation_time,
        "" as doc_date,
        "" as operation_emp_name,
        invoice_type_name,
        0 as taxt_amount,
        0 as original_tax_amount,
        "" as js_times,
        "" as offset_day,
        "" as meet_lvl_name,
        "" as meet_type_name,
        0 as buget_limit,
        0 as sum_person,
        "" as invo_number,
        "" as invo_code,
        "" as city,
        0 as amounttax,
        "" as offset_ratio,
        "" as amounttax_ratio,
        "" as approve_name,
        "" as ratio,
        importdate
            FROM 01_datamart_layer_007_h_cw_df.finance_rma_travel_accomm
        WHERE {condition_sql}
            """.format(condition_sql=condition_sql).replace("\n", "").replace("\r", "").strip()

        # print(sql)

        try:
            start_time = time.perf_counter()
            prod_execute_sql(conn_type=CONN_TYPE, sqltype="insert", sql=sql)
            consumed_time = round(time.perf_counter() - start_time)
            print(f"*** 执行SQL耗时 {consumed_time} sec")
        except Exception as e:
            print(e)
            raise RuntimeError(e)


check13_service = Check13Service()
check13_service.save_fee_data()  # 保存数据总数 5917850
check13_service.analyze_data(coefficient=4)  # 执行检查点13的数据共耗时 2167 sec
print("--- ok, check_13 has been completed ---")
