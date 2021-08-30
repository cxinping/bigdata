import sys
sys.path.append("/usr/local/lib64/python3.6/site-packages")

import jaydebeapi
import jpype
import os
import traceback

conn = None
database_list = None
jars_path = '/you_filed_algos/jars/'

dirver = "org.apache.hive.jdbc.HiveDriver"
url = "jdbc:hive2://bigdata-dev-014:7180/;ssl=true;sslTrustStore=/you_filed_algos/cm-auto-global_truststore.jks;principal=impala/bigdata-dev-014@SJFWPT.SINOPEC.COM"

jars_file_ls = []
jars_file_str = ''
for jar in os.listdir(jars_path):
    jars_file_ls.append(jars_path + jar)

jars_file_str = ':'.join(jars_file_ls)


jvm_options  = "-Djava.class.path=" + jars_file_str

#jvm = jpype.getDefaultJVMPath()
jvm = '/you_filed_algos/jdk8/jre/lib/amd64/server/libjvm.so'


if not jpype.isJVMStarted():
    try:
        #print('--------startjvm---------')
        jpype.startJVM(jvm, jvm_options )
        #print("JVM path:"+ jpype.getDefaultJVMPath())
        #print('----- running jvm -------------')

    except Exception as e:
        print('====== throw error ======')
        traceback.print_exc()
        jpype.shutdownJVM()

    System = jpype.java.lang.System
    #System = jpype.JClass('java.lang.System')
    System.setProperty("java.security.krb5.conf", "/you_filed_algos/krb5.conf")

    Configuration = jpype.JPackage('org.apache.hadoop.conf').Configuration
    conf = Configuration()
    conf.set("hadoop.security.authentication", "kerberos")

    try:
        UserGroupInformation = jpype.JClass('org.apache.hadoop.security.UserGroupInformation')
        UserGroupInformation.setConfiguration(conf)
        UserGroupInformation.loginUserFromKeytab("sjfw_pbpang", "/you_filed_algos/sjfw_pbpang.keytab")

        conn = jaydebeapi.connect(dirver, url)
        print("* create connection object")

        cur = conn.cursor()
        cur.execute('show databases')
        database_list = cur.fetchall()
        print('*** query results ***')
        for data in database_list:
            print(data)

        cur.close()
        conn.close()
    except Exception as ex:
        traceback.print_exc()


    jpype.shutdownJVM()
