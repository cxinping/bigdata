# -*- coding: utf-8 -*-

import jpype
import os
import shutil
import time
import traceback
from datetime import datetime


class HDFSTools(object):

    def __init__(self, conn_type='prod'):
        conf_path = r'/you_filed_algos'
        jars_path = r'/you_filed_algos/jars/'
        jars_file_ls = []
        # jars_file_str = ''
        for jar in os.listdir(jars_path):
            jars_file_ls.append(jars_path + jar)

        jars_file_str = ':'.join(jars_file_ls)
        jvm_options = "-Djava.class.path=" + jars_file_str
        # jvm = jpype.getDefaultJVMPath()
        jvm = '/you_filed_algos/jdk8/jre/lib/amd64/server/libjvm.so'

        try:
            if not jpype.isJVMStarted():
                jpype.startJVM(jvm, jvm_options)

            Configuration = jpype.JClass('org.apache.hadoop.conf.Configuration')
            conf = Configuration()
            Path = jpype.JClass('org.apache.hadoop.fs.Path')
            PROD = 'prod'
            TEST = 'test'

            if conn_type == PROD:
                # 生产环境
                conf.addResource(Path(conf_path + "/core-site.xml"))
                conf.addResource(Path(conf_path + "/hdfs-site.xml"))
            elif conn_type == TEST:
                # 测试环境
                conf.addResource(Path(conf_path + "/core-site_kaifa.xml"))
                conf.addResource(Path(conf_path + "/hdfs-site_kaifa.xml"))

            conf.set('hadoop.security.authorization', 'true')
            conf.set('hadoop.security.authentication', 'kerberos')

            System = jpype.java.lang.System

            if conn_type == PROD:
                # 生产环境
                System.setProperty("java.security.krb5.conf", "/you_filed_algos/prod-krb5.conf")
            elif conn_type == TEST:
                # 测试环境
                System.setProperty("java.security.krb5.conf", "/you_filed_algos/krb5_kaifa.conf")

            UserGroupInformation = jpype.JClass('org.apache.hadoop.security.UserGroupInformation')
            UserGroupInformation.setConfiguration(conf)

            self.conf = conf
            print('*** HDFSTools init conn_type=', conn_type)

            if conn_type == PROD:
                # 生产环境
                UserGroupInformation.loginUserFromKeytab("sjfw_wangsh12348", "/you_filed_algos/sjfw_wangsh12348.keytab")
            elif conn_type == TEST:
                # 测试环境
                UserGroupInformation.loginUserFromKeytab("sjfw_wangsh12348",
                                                         "/you_filed_algos/sjfw_wangsh12348_kaifa.keytab")

            FileSystem = jpype.JClass('org.apache.hadoop.fs.FileSystem')
            self.fs = FileSystem.get(conf)
        except Exception as e:
            print('====== throw error ======')
            print(e)
            traceback.print_exc()
            raise RuntimeError(e)

    def uploadFile(self, hdfsDirPath, localPath):
        print('* begin uploadFile *')
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
            print('* end uploadFile *')
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

    def downLoadDir(self, hdfsDirUrl, localDirUrl):
        print('--- downLoadDir ---')

        Path = jpype.JClass('org.apache.hadoop.fs.Path')
        File = jpype.JClass('java.io.File')
        path = Path(hdfsDirUrl)
        file = File(localDirUrl)

        if not file.exists() and not file.isDirectory():
            file.mkdirs()

        try:
            fsArr = self.fs.listStatus(path)
            for fss in fsArr:
                name = fss.getPath().getName()
                print(fss.getPath())

                if fss.isFile():
                    print('222 ', file.getPath() + "/" + name)
                    self.downLoadFile(fss.getPath().toString(), file.getPath() + "/" + name)

        except Exception as e:
            print(e)
            traceback.print_exc()

    def downLoadDir_recursion(self, hdfsDirUrl, localDirUrl):
        """
        递归下载文件加下的内容
        :param hdfsDirUrl:
        :param localDirUrl:
        :return:
        """
        print('--- downLoadDir_recursion ---')
        Path = jpype.JClass('org.apache.hadoop.fs.Path')
        File = jpype.JClass('java.io.File')
        path = Path(hdfsDirUrl)
        file = File(localDirUrl)

        if not file.exists() or not file.isDirectory():
            file.mkdirs()

        try:
            fsArr = self.fs.listStatus(path)
            hdfsFileUrl_ls = []

            for fss in fsArr:
                # 遍历文件列表，判断是文件还是文件夹
                self.isDir(fss, hdfsFileUrl_ls)

            # print('*** 处理任务数 ==> ', len(hdfsFileUrl_ls))

            # hdfsFileUrl_ls = hdfsFileUrl_ls[0:1000]

            # 单线程下载
            # x = datetime.now()
            # for hdfs_file_url in hdfsFileUrl_ls:
            #     #print(hdfs_file_url)
            #     local_file_name = hdfs_file_url.replace('hdfs://nameservice1/', localDirUrl)
            #     print('* hdfs_file_url=> ',hdfs_file_url)
            #     print('* local_file_name=> ',local_file_name)
            #
            #     self.downLoadFile(hdfs_file_url, local_file_name)
            #     print('')
            # print('共耗时' + str(datetime.now() - x))

            # 多线程下载
            # threadPool = ThreadPoolExecutor(max_workers=60)
            # x = datetime.now()
            # obj_list = []
            # for hdfs_file_url in hdfsFileUrl_ls:
            #     local_file_name = hdfs_file_url.replace('hdfs://nameservice1/', localDirUrl)
            #     print('* hdfs_file_url=> ', hdfs_file_url)
            #     print('* local_file_name=> ', local_file_name)
            #     print('')
            #     obj = threadPool.submit(self.downLoadFile, hdfs_file_url, local_file_name)
            #     obj_list.append(obj)
            #
            # for future in as_completed(obj_list):
            #     data = future.result()
            #     print(data)
            #
            # threadPool.shutdown(wait=True)
            # print('共耗时' + str(datetime.now() - x))

            return hdfsFileUrl_ls
        except Exception as e:
            print(e)
            traceback.print_exc()

    def isDir(self, fileStatus, hdfsFileUrl_ls):
        # 如果是文件夹，则获取该文件夹下的文件列表，遍历判断递归调用

        if fileStatus.isDirectory():
            dirname = fileStatus.getPath().getName()
            dirname = fileStatus.getPath().toString()
            # print(dirname, fileStatus.getPath().toString())
            Path = jpype.JClass('org.apache.hadoop.fs.Path')
            try:
                listStatus = self.fs.listStatus(Path(str(dirname)))
                for fileStatus2 in listStatus:
                    self.isDir(fileStatus2, hdfsFileUrl_ls)
            except Exception as e:
                print(e)
                traceback.print_exc()
        else:
            dirname = fileStatus.getPath().toString()
            # print('* filename ==> ' , dirname)
            hdfsFileUrl_ls.append(dirname)

    def downLoadFile(self, hdfsUrl, localUrl):
        print('--- begin downLoadFile ---')
        fin = None
        fout = None
        try:
            Path = jpype.JClass('org.apache.hadoop.fs.Path')
            FSDataInputStream = jpype.JClass('org.apache.hadoop.fs.FSDataInputStream')
            FileOutputStream = jpype.JClass('java.io.FileOutputStream')
            IOUtils = jpype.JClass('org.apache.commons.io.IOUtils')
            path = Path(hdfsUrl)

            # 判断本地文件所在文件夹是否存在，如果不存在就先创建文件夹
            File = jpype.JClass('java.io.File')
            file = File(localUrl)
            file_dir = File(file.getParent())

            if not bool(file_dir.exists()):
                file_dir.mkdirs()

            status = self.fs.getFileStatus(path)
            print(status)

            if status is not None and status.isFile():
                # print('*** it is a file')
                fin = self.fs.open(path)
                fout = FileOutputStream(localUrl)
                IOUtils.copy(fin, fout)

                fout.flush()
            print('--- end downLoadFile ---')
            return f'downlaod from {hdfsUrl} to {localUrl}'
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

    def downLoadFile2(self, hdfsUrl, localUrl):
        """
        从集群中下载HDFS文件
        :param hdfsUrl:
        :param localUrl:
        :return:
        """
        print('--- begin downLoadFile2 ---')
        fin = None
        fout = None

        try:
            Path = jpype.JClass('org.apache.hadoop.fs.Path')
            #FSDataInputStream = jpype.JClass('org.apache.hadoop.fs.FSDataInputStream')
            FileOutputStream = jpype.JClass('java.io.FileOutputStream')
            IOUtils = jpype.JClass('org.apache.hadoop.io.IOUtils')
            path = Path(hdfsUrl)
            print('1_1 hdfsUrl=> ' , hdfsUrl)

            # 判断本地文件所在文件夹是否存在，如果不存在就先创建文件夹
            File = jpype.JClass('java.io.File')
            file = File(localUrl)
            file_dir = File(file.getParent())

            print('1_2 file_dir =>' , file_dir)

            # if not bool(file_dir.exists()):
            #     file_dir.mkdirs()
            #     print('2 file make dirs')

            file_dir_str = str(file_dir.getAbsolutePath())
            print('1_3 file_dir_str =>', file_dir_str)

            if not os.path.exists(file_dir_str):
                os.makedirs(file_dir_str)
                print(f'2_1 make dirs {file_dir_str}')
            else:
                print(f'2_2 dirs {file_dir_str} exists')

            print('3_1 path => ', path)
            # status = self.fs.getFileStatus(path)
            # print('3_2 status => ',status, type(status))

            #if status is not None and status.isFile():
            fin = self.fs.open(path)
            print('4 fin=', fin)

            fout = FileOutputStream(localUrl)
            #print('5 fout=', fout)

            IOUtils.copyBytes(fin, fout, 1024 * 1024 * 100, jpype.java.lang.Boolean(False) )  # 带缓冲的下载文件，hdfs文件最大 250M

            # print('6_1 fin=', fin)
            # print('6_2 fout=', fout)

            if fout:
                print('download file form HDFS')
                fout.flush()

            IOUtils.closeStream(fin)
            IOUtils.closeStream(fout)
            print('--- end downLoadFile2 ---')
            return f'downlaod from {hdfsUrl} to {localUrl}'
        except Exception as e:
            print(e)
            traceback.print_exc()

    def downLoadFile3(self, hdfsUrl, localUrl):
        print('--- begin downLoadFile3 ---')
        fin = None
        fout = None

        try:
            Path = jpype.JClass('org.apache.hadoop.fs.Path')
            FSDataInputStream = jpype.JClass('org.apache.hadoop.fs.FSDataInputStream')
            FileOutputStream = jpype.JClass('java.io.FileOutputStream')
            IOUtils = jpype.JClass('org.apache.hadoop.io.IOUtils')
            path = Path(hdfsUrl)

            # 判断本地文件所在文件夹是否存在，如果不存在就先创建文件夹
            File = jpype.JClass('java.io.File')
            file = File(localUrl)
            file_dir = File(file.getParent())

            if not bool(file_dir.exists()):
                file_dir.mkdirs()

            status = self.fs.getFileStatus(path)
            # print(status)

            if status is not None and status.isFile():
                srcPath = Path(hdfsUrl)
                dstPath = Path(localUrl)
                self.fs.copyToLocalFile(srcPath, dstPath)
            print('--- end downLoadFile3 ---')
            return f'downlaod from {hdfsUrl} to {localUrl}'
        except Exception as e:
            print(e)
            traceback.print_exc()

    # def delete(self, hdfsDirPath):
    #     print('---- delete ----')
    #
    #     try:
    #         Path = jpype.JClass('org.apache.hadoop.fs.Path')
    #         flag = self.fs.delete(Path(hdfsDirPath), True)
    #         return flag
    #     except Exception as e:
    #         print(e)
    #         traceback.print_exc()

    def ls(self, url='/user/sjfw_wangsh12348/test_data/'):
        print('---- hdfs ls ----')
        Path = jpype.JClass('org.apache.hadoop.fs.Path')
        try:
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


def prod_demo1():
    hdfs = HDFSTools(conn_type='prod')

    hdfsDirPath = 'hdfs:///user/sjfw_wangsh12348/test_data/'
    # 对外挂载地址地址 /public_filed_algos/report/check_02_trip_data.json
    # docker 容器地址  /my_filed_algos/check_02_trip_data.json
    localPath = r'/my_filed_algos/test.txt'
    # upload file to HDFS
    # hdfs.uploadFile(hdfsDirPath, localPath)

    # list HDFS files
    # hdfs.ls(url=hdfsDirPath)

    # delete HDFS file
    del_hdfs_path = 'hdfs:///user/sjfw_wangsh12348/test_data/test.txt'
    # hdfs.delete(del_hdfs_path)

    # download from HDFS
    # hdfs.downLoadFile(hdfsUrl='hdfs://nameservice1/user/hive/warehouse/03_basal_layer_zfybxers00.db/RFM_POST_VOUCHER/importdate=20210909/20210909182437', localUrl='/my_filed_algos/prod_kudu_data/20210909182437')

    for i in range(20):
        print('*** index => ',i)
        time.sleep(0.1)
        hdfs.downLoadFile2(
            hdfsUrl='hdfs:///user/hive/warehouse/03_basal_layer_zfybxers00.db/zfybxers00_z_rma_assist_object_m/importdate=20210927/000007_0',
            localUrl='/my_filed_algos/prod_kudu_data/000007_0')

    hdfs.shutdownJVM()
    print('--- ok ---')

def prod_demo2():
    hdfs = HDFSTools(conn_type='prod')

    hdfsDirPath = 'hdfs:///user/hive/warehouse/'
    # list HDFS files
    # hdfs.ls(url=hdfsDirPath)

    # 下载 HDFS 上的单个文件
    hdfs.downLoadFile(hdfsUrl='hdfs:///user/hive/warehouse/02_logical_layer_003_z_lf_cw.db/zccw0101_m/importdate=20210921/000000_0',
                      localUrl='/my_filed_algos/prod_kudu_data/000000_0')

    # 递归下载 HDFS 上的文件夹里的文件
    # hdfsFileUrl_ls = hdfs.downLoadDir_recursion(hdfsDirUrl='hdfs:///user/hive/warehouse/03_basal_layer_zfybxers00.db',
    #                                             localDirUrl='/my_filed_algos/prod_kudu_data/')
    #
    # for hdfs_file_url in hdfsFileUrl_ls:
    #     print(hdfs_file_url)

    hdfs.shutdownJVM()
    print('--- ok , completed work ---')


def prod_demo3():
    hdfs = HDFSTools(conn_type='prod')
    # 下载 HDFS 上的单个文件
    hdfs.downLoadFile(hdfsUrl='hdfs://nameservice1/user/hive/warehouse/02_logical_layer_001_o_lf_cw.db/BIC/AOCCW01012/importdate=20211112/20211112184435=00001',
                      localUrl='/my_filed_algos/prod_kudu_data/20211112184435=00001')

    hdfs.shutdownJVM()
    print('--- ok , completed work ---')



def test_demo1():
    try:
        hdfs = HDFSTools(conn_type='test')
        hdfsDirPath = 'hdfs:///user/sjfw_wangsh12348/'
        # hdfs.ls(url=hdfsDirPath)

        hdfsDirPath = 'hdfs:///user/sjfw_wangsh12348/'
        # 对外挂载地址地址 /public_filed_algos/report/check_02_trip_data.json
        # docker 容器地址  /my_filed_algos/check_02_trip_data.json
        localPath = r'/my_filed_algos/prod_kudu_data/a.txt'
        # # upload file to HDFS
        hdfs.uploadFile(hdfsDirPath, localPath)

        #hdfs.shutdownJVM()

        print('--- ok ---')
    except Exception as e:
        print(e)


# test_hdfs = HDFSTools(conn_type='test')

def main():
    prod_hdfs = HDFSTools(conn_type='prod')

    # 递归下载 HDFS 上的文件夹里的文件
    # /user/hive/warehouse/02_logical_layer_001_o_lf_cw.db/occw0101_m hdfs:///user/hive/warehouse/02_logical_layer_001_o_lf_cw.db/occw0101_m
    # /user/hive/warehouse/03_basal_layer_zfybxers00.db hdfs:///user/hive/warehouse/03_basal_layer_zfybxers00.db
    hdfsDirUrl = 'hdfs:///user/hive/warehouse/03_basal_layer_zfybxers00.db'
    localDirUrl = '/my_filed_algos/prod_kudu_data/'

    print('* part1 bbbccc ')
    hdfsFileUrl_ls = prod_hdfs.downLoadDir_recursion(hdfsDirUrl=hdfsDirUrl,
                                                     localDirUrl=localDirUrl)
    print('* part2 ')
    print('*** 处理文件数 ==> ', len(hdfsFileUrl_ls))

    if os.path.exists(localDirUrl + 'user'):
        shutil.rmtree(localDirUrl + 'user')

    test_hdfs = HDFSTools(conn_type='test')

    x = datetime.now()
    for index, hdfs_file_url in enumerate(hdfsFileUrl_ls):
        # if os.path.exists(localDirUrl + 'user'):
        #     shutil.rmtree(localDirUrl + 'user')

        hdfs_file_url = str(hdfs_file_url)
        print(f'处理HDFS文件 {len(hdfsFileUrl_ls)} , hdfsFileUrl_ls index => {index}')
        print('prod hdfs_file_url => ', hdfs_file_url)
        local_file_name = hdfs_file_url.replace('hdfs://nameservice1/', localDirUrl)
        print('local_file_name => ', local_file_name)
        hdfs_file_url = hdfs_file_url.replace('hdfs://nameservice1/user', 'hdfs:///user')
        print('test hdfs_file_url => ', hdfs_file_url)

        time.sleep(1)
        prod_hdfs.downLoadFile(hdfs_file_url, local_file_name)
        time.sleep(1)
        test_hdfs.uploadFile(hdfsDirPath=hdfs_file_url, localPath=local_file_name)

        if os.path.exists(local_file_name):
            os.remove(local_file_name)
            print(f'delete file {local_file_name}')

        print('')

    print('共耗时' + str(datetime.now() - x))

    # 多线程 1，从生产集群下载文件 2, 向开发集群上传文件
    # threadPool = ThreadPoolExecutor(max_workers=60)
    # x = datetime.now()
    # obj_list = []
    #
    # for hdfs_file_url in hdfsFileUrl_ls:
    #     print('hdfs_file_url => ', hdfs_file_url)
    #     local_file_name = hdfs_file_url.replace('hdfs://nameservice1/', localDirUrl)
    #     print('local_file_name => ', local_file_name)
    #     obj = threadPool.submit(exec_task, prod_hdfs, test_hdfs, hdfs_file_url, local_file_name)
    #     obj_list.append(obj)
    #
    # for future in as_completed(obj_list):
    #     data = future.result()
    #     print(data)
    #
    # threadPool.shutdown(wait=True)
    # print('共耗时' + str(datetime.now() - x))

    prod_hdfs.shutdownJVM()
    print('--- ok , completed work ---')


def exec_task(prod_hdfs, test_hdfs, hdfs_file_url, local_file_name):
    prod_hdfs.downLoadFile(hdfs_file_url, local_file_name)
    test_hdfs.uploadFile(hdfsDirPath=hdfs_file_url, localPath=str(local_file_name))
    os.remove(str(local_file_name))

    return f'from {local_file_name} to {hdfs_file_url}'


# def danger_test():
#     hdfs = HDFSTools(conn_type='prod')
    # del_hdfs_path2 = 'hdfs:///user/hive/warehouse/test_database_20210925.db/test_delete_file_new_3'
    # hdfs.delete(del_hdfs_path2)
    #
    # hdfsFileUrl_ls = hdfs.downLoadDir_recursion(hdfsDirUrl='hdfs:///user/hive/warehouse/test_database_20210925.db',
    #                                             localDirUrl='/my_filed_algos/prod_kudu_data/')

    # if hdfsFileUrl_ls:
    #     for hdfs_file_url in hdfsFileUrl_ls:
    #         print(hdfs_file_url)
    #         hdfs.delete(hdfs_file_url)


if __name__ == "__main__":
    #danger_test()

    #prod_demo1()

    #test_demo1()

    prod_demo3()

    # main()

    pass
