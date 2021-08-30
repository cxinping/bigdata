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

pd.set_option('display.max_columns', None)   # 显示完整的列
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

    if not jpype.isJVMStarted():
        try:
            #print('--------startjvm---------')
            jpype.startJVM(jvm, jvm_options)
            # print("JVM path:"+ jpype.getDefaultJVMPath())
            # print('----- running jvm -------------')

        except Exception as e:
            print('====== throw error ======')
            traceback.print_exc()
            jpype.shutdownJVM()

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

            # 关闭游标
            cur.close()
            # 关闭连接
            conn.close()
            return list
        except Exception as ex:
            print(ex)
            traceback.print_exc()


def prod_execute_sql(  sqltype='insert', sql='' ):
    is_commit = False
    conn = None
    database_list = None
    jars_path = '/you_filed_algos/jars/'
    dirver = "org.apache.hive.jdbc.HiveDriver"
    # url = "jdbc:hive2://bigdata-dev-014:7180/;ssl=true;sslTrustStore=/you_filed_algos/cm-auto-global_truststore.jks;principal=impala/bigdata-dev-014@SJFWPT.SINOPEC.COM"
    # url = "jdbc:hive2://hadoop-pro-017:7180/default;ssl=true;sslTrustStore=/you_filed_algos/prod-cm-auto-global_truststore.jks;principal=impala/hadoop-pro-017@BYHW.HADOOP.COM"
    is_prod_env = True

    if is_prod_env:
        url = "jdbc:hive2://hadoop-pro-017:7180/default;ssl=true;sslTrustStore=/you_filed_algos/prod-cm-auto-global_truststore.jks;principal=impala/hadoop-pro-017@BYHW.HADOOP.COM"
    else:
        url = "jdbc:hive2://bigdata-dev-014:7180/;ssl=true;sslTrustStore=/you_filed_algos/cm-auto-global_truststore.jks;principal=impala/bigdata-dev-014@SJFWPT.SINOPEC.COM"

    jars_file_ls = []
    jars_file_str = ''
    for jar in os.listdir(jars_path):
        jars_file_ls.append(jars_path + jar)

    jars_file_str = ':'.join(jars_file_ls)

    jvm_options = "-Djava.class.path=" + jars_file_str

    # jvm = jpype.getDefaultJVMPath()
    jvm = '/you_filed_algos/jdk8/jre/lib/amd64/server/libjvm.so'

    if not jpype.isJVMStarted():
        try:
            # print('--------startjvm---------')
            jpype.startJVM(jvm, jvm_options)
            # print("JVM path:"+ jpype.getDefaultJVMPath())
            # print('----- running jvm -------------')

        except Exception as e:
            print('====== throw error ======')
            traceback.print_exc()
            jpype.shutdownJVM()

        System = jpype.java.lang.System

        # System.setProperty("java.security.krb5.conf", "/you_filed_algos/krb5.conf")
        # System.setProperty("java.security.krb5.conf", "/you_filed_algos/prod-krb5.conf")

        if is_prod_env:
            System.setProperty("java.security.krb5.conf", "/you_filed_algos/prod-krb5.conf")
        else:
            System.setProperty("java.security.krb5.conf", "/you_filed_algos/krb5.conf")

        Configuration = jpype.JPackage('org.apache.hadoop.conf').Configuration
        conf = Configuration()
        conf.set("hadoop.security.authentication", "kerberos")

        try:
            UserGroupInformation = jpype.JClass('org.apache.hadoop.security.UserGroupInformation')
            UserGroupInformation.setConfiguration(conf)
            # UserGroupInformation.loginUserFromKeytab("sjfw_pbpang", "/you_filed_algos/sjfw_pbpang.keytab")
            # UserGroupInformation.loginUserFromKeytab("sjfw_wangsh12348", "/you_filed_algos/sjfw_wangsh12348.keytab")

            if is_prod_env:
                UserGroupInformation.loginUserFromKeytab("sjfw_wangsh12348", "/you_filed_algos/sjfw_wangsh12348.keytab")
            else:
                UserGroupInformation.loginUserFromKeytab("sjfw_pbpang", "/you_filed_algos/sjfw_pbpang.keytab")

            conn = jaydebeapi.connect(dirver, url)
            # print("* create connection object")

            cur = conn.cursor()

            if sqltype == 'insert':
                cur.execute(sql)
                #conn.commit() # hive 不支持 commit
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

def dis_connection():
    if jpype.isJVMStarted():
        #print('=== shutdown JVM===')
        jpype.shutdownJVM()

def getKUDUdata(sql):
    records = prod_execute_sql(sql)
    dataFromHana = []
    dataFfromHana1 = []
    dataFfromHana2 = []

    for item in records:
        #print(type(item), item)
        dataFromHana.append( item )
        #dataFfromHana1.append(list(item))
        #dataFfromHana2.append(item[1])

    print(dataFromHana)

    df = pd.DataFrame(dataFromHana)
    return df


if __name__ == "__main__":
    start_time = time.perf_counter()
    # 连接 KUDU 库下的表
    #rd_df = getKUDUdata('select * from python_test_kudu.irisdataset limit 10')
    # 连接 HIVE 表
    #rd_df = getKUDUdata('select * from python_test.irisdataset limit 100')

    # 连接生产库
    rd_df = getKUDUdata('select * from 03_basal_layer_zfybxers00.ZFYBXERS00_Z_RMA_BASE_APPLY limit 3')

    print(rd_df)
    print( f'* rd_df ==> {rd_df.shape[0]} rows * {rd_df.shape[1]} columns')
    consumed_time = round(time.perf_counter() - start_time)
    print(f'* consumed_time={consumed_time}')
    dis_connection()
    print('-- ok --')




