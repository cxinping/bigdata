# -*- coding: utf-8 -*-

import jaydebeapi
import jpype
import os
import traceback
import pandas as pd
import time
from jpype import JString


class HDFSTools(object):

    def __init__(self):
        conf_path = r'/you_filed_algos'
        jars_path = r'/you_filed_algos/jars/'
        jars_file_ls = []
        jars_file_str = ''
        for jar in os.listdir(jars_path):
            jars_file_ls.append(jars_path + jar)

        jars_file_str = ':'.join(jars_file_ls)
        jvm_options = "-Djava.class.path=" + jars_file_str
        # jvm = jpype.getDefaultJVMPath()
        jvm = '/you_filed_algos/jdk8/jre/lib/amd64/server/libjvm.so'

        try:
            jpype.startJVM(jvm, jvm_options)
            Configuration = jpype.JClass('org.apache.hadoop.conf.Configuration')
            conf = Configuration()
            Path = jpype.JClass('org.apache.hadoop.fs.Path')
            conf.addResource(Path(conf_path + "/core-site.xml"))
            conf.addResource(Path(conf_path + "/hdfs-site.xml"))
            conf.set('hadoop.security.authorization', 'true')
            conf.set('hadoop.security.authentication', 'kerberos')

            System = jpype.java.lang.System
            PROD = 'wangsh12348'
            TEST = 'pbpang'
            conn_type = 'wangsh12348'
            if conn_type == PROD:
                System.setProperty("java.security.krb5.conf", "/you_filed_algos/prod-krb5.conf")
            elif conn_type == TEST:
                System.setProperty("java.security.krb5.conf", "/you_filed_algos/krb5.conf")

            UserGroupInformation = jpype.JClass('org.apache.hadoop.security.UserGroupInformation')
            UserGroupInformation.setConfiguration(conf)

            if conn_type == PROD:
                UserGroupInformation.loginUserFromKeytab("sjfw_wangsh12348", "/you_filed_algos/sjfw_wangsh12348.keytab")
            elif conn_type == TEST:
                UserGroupInformation.loginUserFromKeytab("sjfw_pbpang", "/you_filed_algos/sjfw_pbpang.keytab")

            FileSystem = jpype.JClass('org.apache.hadoop.fs.FileSystem')
            self.fs = FileSystem.get(conf)
        except Exception as e:
            print('====== throw error ======')
            print(e)
            traceback.print_exc()

    def uploadFile(self, hdfsDirPath, localPath):
        print('---- begin uploadFile ----')
        fin = None
        fout = None
        File = jpype.JClass('java.io.File')
        FileInputStream = jpype.JClass('java.io.FileInputStream')
        Path = jpype.JClass('org.apache.hadoop.fs.Path')
        IOUtils = jpype.JClass('org.apache.commons.io.IOUtils')
        try:
            file = File(localPath)
            fin = FileInputStream(file)

            print('localPath ==> ', localPath)
            print('upload file to HDFS ==> ', hdfsDirPath + str(file.getName()))

            fout = self.fs.create(Path(hdfsDirPath + str(file.getName())))
            IOUtils.copy(fin, fout)
            fout.flush()
            print('---- end uploadFile ----')
        except Exception as e:
            print(e)
            traceback.print_exc()
        finally:
            try:
                if fin:
                    fin.close()
                if fout:
                    fout.close()
            except Exception as e2:
                print(e2)
                traceback.print_exc()

    def ls(self):
        Path = jpype.JClass('org.apache.hadoop.fs.Path')
        try:
            url = '/user/sjfw_wangsh12348/test_data/'
            path = Path(url)
            fsArr = self.fs.listStatus(path)
            for fss in fsArr:
                print('fss.getPath().getName()=> ', fss.getPath().getName())
                print('fss.getLen()=> ', fss.getLen())
                print('fss.getOwner()=> ', fss.getOwner())
                print('fss.getGroup()=> ', fss.getGroup())
                ts = int(fss.getModificationTime())
                print('fss.getModificationTime()=> ', ts, self.timeStamp(ts))
                print('fss.getPermission()=> ', fss.getPermission().toString())
                print('fss.isDirectory()=> ', fss.isDirectory())
                print('')
        except Exception as e:
            print(e)

    def timeStamp(self, timeNum):
        timeStamp = float(timeNum / 1000)
        timeArray = time.localtime(timeStamp)
        otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
        return otherStyleTime


    def shutdownJVM(self):
        if jpype.isJVMStarted():
            jpype.shutdownJVM()


if __name__ == "__main__":
    hdfs = HDFSTools()

    hdfsDirPath = 'hdfs:///user/sjfw_wangsh12348/test_data/'
    # 对外挂载地址地址 /public_filed_algos/report/check_02_trip_data.json
    # docker 容器地址  /my_filed_algos/check_02_trip_data.json
    localPath = r'/my_filed_algos/check_02_trip_data.json'

    #hdfs.uploadFile(hdfsDirPath, localPath)
    hdfs.ls()

    hdfs.shutdownJVM()



