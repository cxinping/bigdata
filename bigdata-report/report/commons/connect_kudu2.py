# -*- coding: utf-8 -*-

import sys
sys.path.append("/usr/local/lib64/python3.6/site-packages")

import jaydebeapi
import jpype
import os
import traceback
from report.commons.logging import get_logger

log = get_logger(__name__)


def prod_execute_sql(conn_type='prod', sqltype='insert', sql=''):
    """
    :param conn_type: 连接类型
                    prod 生产集群环境
                    test 测试集群环境
    :param sqltype:
    :param sql:
    :return:
    """
    jars_path = '/you_filed_algos/jars/'
    dirver = "org.apache.hive.jdbc.HiveDriver"
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

    try:
        if not jpype.isJVMStarted():
            #log.info(sql)
            log.info('----- 1 start jvm ----- ')
            jpype.startJVM(jvm, jvm_options)

            # jpype.startJVM(jvm, "-ea", jvm_options, '-Xmx5g', '-Xms5g', '-Xmn2g', '-XX:+UseParNewGC',
            #                '-XX:ParallelGCThreads=8', '-XX:SurvivorRatio=6', '-XX:+UseConcMarkSweepGC')

            log.info('----- 2 attaching jvm ----- ')
            jpype.attachThreadToJVM()

        #if not jpype.isThreadAttachedToJVM():
            #print('-----attaching jvm-----')
            #jpype.attachThreadToJVM()
            # jpype.java.lang.Thread.currentThread().setContextClassLoader(
            #     jpype.java.lang.ClassLoader.getSystemClassLoader()
            # )

        # print("JVM path:"+ jpype.getDefaultJVMPath())
        #log.info('----- running jvm -------------')
    except Exception as e:
        log.error('====== throw error ======')
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
    try:
        #print(f'* jpype.isJVMStarted()= > {jpype.isJVMStarted()}')
        if jpype.isJVMStarted():
            print('=== shutdown JVM===')
            jpype.shutdownJVM()
    except Exception as ex:
        print(ex)
        traceback.print_exc()
        raise RuntimeError(ex)