# -*- coding: utf-8 -*-

import jaydebeapi
import jpype
import os
import traceback
import pandas as pd
import time

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

            conf = jpype.JClass('org.apache.hadoop.conf.Configuration')
            print(conf, conf_path + "/core-site.xml")
            #conf.addResource(str(conf_path + "/core-site.xml"))
            #conf.addResource(conf_path + "/hdfs-site.xml")

            conf.set('hadoop.security.authorization' , 'true')
            conf.set('hadoop.security.authentication' , 'kerberos')

        except Exception as e:
            print('====== throw error ======')
            print(e)
            traceback.print_exc()

    def shutdownJVM(self):
        if jpype.isJVMStarted():
            jpype.shutdownJVM()

if __name__ == "__main__":
    hdfs = HDFSTools()

    hdfs.shutdownJVM()


