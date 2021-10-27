# -*- coding: utf-8 -*-

import sys

sys.path.append("/usr/local/lib64/python3.6/site-packages")

import jaydebeapi
import jpype
import os
import traceback
import pandas as pd
import time

"""
Created on 2021-08-05

@author: Wang Shuo


"""

pd.set_option('display.max_columns', None)  # 显示完整的列
pd.set_option('display.max_rows', None)  # 显示完整的行
pd.set_option('display.expand_frame_repr', False)  # 设置不折叠数据


def execute_sql(sql):
    conn = None
    jars_path = '/you_filed_algos/jars/'

    dirver = "org.apache.hive.jdbc.HiveDriver"
    url = "jdbc:hive2://bigdata-dev-014:7180/;ssl=true;sslTrustStore=/you_filed_algos/cm-auto-global_truststore.jks;principal=impala/bigdata-dev-014@SJFWPT.SINOPEC.COM"

    jars_file_ls = []
    jars_file_str = ''
    for jar in os.listdir(jars_path):
        jars_file_ls.append(jars_path + jar)

    jars_file_str = ':'.join(jars_file_ls)

    jvm_options = "-Djava.class.path=" + jars_file_str

    # jvm = jpype.getDefaultJVMPath()
    jvm = '/you_filed_algos/jdk8/jre/lib/amd64/server/libjvm.so'

    # if not jpype.isJVMStarted():
    # try:
    #     #print('--------startjvm---------')
    #     jpype.startJVM(jvm, jvm_options)
    #     # print("JVM path:"+ jpype.getDefaultJVMPath())
    #     # print('----- running jvm -------------')
    #
    # except Exception as e:
    #     print('====== throw error ======')
    #     traceback.print_exc()
    #     jpype.shutdownJVM()

    try:
        print('--------startjvm---------')
        jpype.startJVM(jvm, jvm_options)
    except Exception as e:
        print('====== throw error ======')
        pass

    System = jpype.java.lang.System
    # System = jpype.JClass('java.lang.System')
    System.setProperty("java.security.krb5.conf", "/you_filed_algos/krb5.conf")

    Configuration = jpype.JPackage('org.apache.hadoop.conf').Configuration
    conf = Configuration()
    conf.set("hadoop.security.authentication", "kerberos")

    try:
        UserGroupInformation = jpype.JClass('org.apache.hadoop.security.UserGroupInformation')
        UserGroupInformation.setConfiguration(conf)
        UserGroupInformation.loginUserFromKeytab("sjfw_pbpang", "/you_filed_algos/sjfw_pbpang.keytab")

        conn = jaydebeapi.connect(dirver, url)
        # print("* create connection object")

        cur = conn.cursor()
        cur.execute(sql)
        list = cur.fetchall()
        # list = cur.fetchone()

        # 关闭游标
        cur.close()
        # 关闭连接
        conn.close()
        return list
    except Exception as ex:
        print(ex)
        traceback.print_exc()


def prod_execute_sql(conn_type='prod', sqltype='insert', sql=''):
    """
    :param conn_type: 连接类型
                    prod 生产环境
                    test 测试环境
    :param sqltype:
    :param sql:
    :return:
    """
    jars_path = '/you_filed_algos/jars/'
    dirver = "org.apache.hive.jdbc.HiveDriver"
    is_prod_env = True
    PROD = 'prod'  # 生产环境
    TEST = 'test'  # 测试环境

    #print('**** prod_execute_sql ****')
    #print('* conn_type=', conn_type)

    if conn_type == PROD:
        # 生产集群使用KUDU
        url = "jdbc:hive2://hadoop-pro-017:7180/default;ssl=true;sslTrustStore=/you_filed_algos/prod-cm-auto-global_truststore.jks;principal=impala/hadoop-pro-017@BYHW.HADOOP.COM"
    elif conn_type == TEST:
        # 开发集群使用KUDU
        # jdbc:hive2://bigdata-dev-014:7180/;ssl=true;sslTrustStore=/home/user/java/keytab/cm-auto-global_truststore.jks;principal=impala/bigdata-dev-014@SJFWPT.SINOPEC.COM
        url = "jdbc:hive2://bigdata-dev-014:7180/;ssl=true;sslTrustStore=/you_filed_algos/cm-auto-global_truststore_kaifa.jks;principal=impala/bigdata-dev-014@SJFWPT.SINOPEC.COM"

    jars_file_ls = []
    jars_file_str = ''
    for jar in os.listdir(jars_path):
        jars_file_ls.append(jars_path + jar)

    jars_file_str = ':'.join(jars_file_ls)

    # jvm_options = ["-Djava.class.path=" + jars_file_str, '-Xmx2G','-Xms512M']
    jvm_options = "-Djava.class.path=" + jars_file_str

    # jvm = jpype.getDefaultJVMPath()
    jvm = '/you_filed_algos/jdk8/jre/lib/amd64/server/libjvm.so'

    # if not jpype.isJVMStarted():
    #     try:
    #         # print('--------startjvm---------')
    #         jpype.startJVM(jvm, jvm_options)
    #         # print("JVM path:"+ jpype.getDefaultJVMPath())
    #         # print('----- running jvm -------------')
    #
    #     except Exception as e:
    #         print('====== throw error ======')
    #         traceback.print_exc()
    #         jpype.shutdownJVM()

    try:
        if not jpype.isJVMStarted():
            # print('--------startjvm---------')
            jpype.startJVM(jvm, "-ea", jvm_options, '-Xmx5g', '-Xms5g', '-Xmn2g', '-XX:+UseParNewGC',
                           '-XX:ParallelGCThreads=8', '-XX:SurvivorRatio=6', '-XX:+UseConcMarkSweepGC')

        if jpype.isJVMStarted() and not jpype.isThreadAttachedToJVM():
            print('-----attaching jvm-----')
            jpype.attachThreadToJVM()
            jpype.java.lang.Thread.currentThread().setContextClassLoader(
                jpype.java.lang.ClassLoader.getSystemClassLoader()
            )

            # print('--------startjvm---------')
        # jpype.startJVM(jvm, jvm_options)
        # print("JVM path:"+ jpype.getDefaultJVMPath())
        # print('----- running jvm -------------')
    except Exception as e:
        # print('====== throw error ======')
        traceback.print_exc()
        raise RuntimeError(e)

    try:
        # print('----- running jvm ，' , jpype.isJVMStarted())

        System = jpype.java.lang.System
        if conn_type == PROD:
            # 生产集群使用KUDU
            System.setProperty("java.security.krb5.conf", "/you_filed_algos/prod-krb5.conf")
        elif conn_type == TEST:
            # 测试集群使用KUDU
            System.setProperty("java.security.krb5.conf", "/you_filed_algos/krb5_kaifa.conf")

        Configuration = jpype.JPackage('org.apache.hadoop.conf').Configuration
        conf = Configuration()
        conf.set("hadoop.security.authentication", "kerberos")

        UserGroupInformation = jpype.JClass('org.apache.hadoop.security.UserGroupInformation')
        UserGroupInformation.setConfiguration(conf)

        if conn_type == PROD:
            # 生产集群使用KUDU
            UserGroupInformation.loginUserFromKeytab("sjfw_wangsh12348", "/you_filed_algos/sjfw_wangsh12348.keytab")
        elif conn_type == TEST:
            # 测试集群使用KUDU
            UserGroupInformation.loginUserFromKeytab("sjfw_wangsh12348", "/you_filed_algos/sjfw_wangsh12348_kaifa.keytab")

        conn = jaydebeapi.connect(dirver, url)
        cur = conn.cursor()

        if sqltype == 'insert':
            cur.execute(sql)
        else:
            cur.execute(sql)
            result = cur.fetchall()

        # 关闭游标
        cur.close()
        # 关闭连接
        conn.close()

        if sqltype != 'insert':
            return result
    except Exception as ex:
        print(ex)
        traceback.print_exc()
        raise RuntimeError(ex)


def dis_connection():
    if jpype.isJVMStarted():
        # print('=== shutdown JVM===')
        jpype.shutdownJVM()


def getKUDUdata(sql):
    records = prod_execute_sql(sqltype='select', sql=sql)
    dataFromHana = []
    dataFfromHana1 = []
    dataFfromHana2 = []

    print(records)

    for item in records:
        # print(type(item), item)
        dataFromHana.append(str(item))
        # dataFfromHana1.append(list(item))
        # dataFfromHana2.append(item[1])

    print(dataFromHana)

    df = pd.DataFrame(dataFromHana)
    return df


if __name__ == "__main__":
    start_time = time.perf_counter()
    # 连接 KUDU 库下的表
    # rd_df = getKUDUdata('select * from python_test_kudu.irisdataset limit 10')
    # 连接 HIVE 表
    # rd_df = getKUDUdata('select * from python_test.irisdataset limit 100')

    # 连接生产库
    # rd_df = getKUDUdata('select finance_travel_id from 01_datamart_layer_007_h_cw_df.finance_travel_bill limit 3')
    # print(rd_df)
    # print( f'* rd_df ==> {rd_df.shape[0]} rows * {rd_df.shape[1]} columns')
    # consumed_time = round(time.perf_counter() - start_time)
    # print(f'* consumed_time={consumed_time}')
    # dis_connection()

    # sql = 'select finance_travel_id from 01_datamart_layer_007_h_cw_df.finance_travel_bill t where t.check_amount > t.jzpz limit 5'
    # print(sql)
    # records = prod_execute_sql(sqltype='select', sql=sql)
    # print('111*** query_kudu_data=>', len(records))
    #
    # sql = 'select finance_travel_id from 01_datamart_layer_007_h_cw_df.finance_travel_bill t where t.check_amount > t.jzpz limit 5'
    # print(sql)
    # records = prod_execute_sql(sqltype='select', sql=sql)
    # print('222*** query_kudu_data=>', len(records))

    try:
        prod_sql = 'select finance_travel_id,bill_id from 01_datamart_layer_007_h_cw_df.finance_travel_bill t limit 5'
        test_sql = 'select * from 01_datamart_layer_007_h_cw_df.payment_result_info limit 5'
        print(test_sql)
        records = prod_execute_sql(conn_type='test', sqltype='select', sql=test_sql)
        print('*** query_kudu_data=>', len(records))
        for record in records:
            print(record)

        dis_connection()
        print('-- ok --')
    except Exception as e:
        print(e)





