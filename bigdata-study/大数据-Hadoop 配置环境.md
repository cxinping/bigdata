

## 安装JDK

1, rpm -qa | grep java
 然后通过 rpm -e --nodeps 后面跟系统自带的jdk名这个命令来删除系统自带的jdk

```	
[root@localhost ~]# rpm -qa | grep java
java-1.7.0-openjdk-headless-1.7.0.261-2.6.22.2.el7_8.x86_64
java-1.8.0-openjdk-1.8.0.272.b10-1.el7_9.x86_64
javapackages-tools-3.4.1-11.el7.noarch
python-javapackages-3.4.1-11.el7.noarch
java-1.8.0-openjdk-headless-1.8.0.272.b10-1.el7_9.x86_64
java-1.7.0-openjdk-1.7.0.261-2.6.22.2.el7_8.x86_64
tzdata-java-2020d-2.el7.noarch
```

然后使用rpm -e- --nodeps 卸载自动自带的jdk
```	
rpm -e --nodeps java-1.7.0-openjdk-headless-1.7.0.261-2.6.22.2.el7_8.x86_64
rpm -e --nodeps java-1.8.0-openjdk-1.8.0.272.b10-1.el7_9.x86_64
rpm -e --nodeps javapackages-tools-3.4.1-11.el7.noarch
rpm -e --nodeps python-javapackages-3.4.1-11.el7.noarch
rpm -e --nodeps java-1.8.0-openjdk-headless-1.8.0.272.b10-1.el7_9.x86_64
rpm -e --nodeps java-1.7.0-openjdk-1.7.0.261-2.6.22.2.el7_8.x86_64
rpm -e --nodeps tzdata-java-2020d-2.el7.noarch
```



2, 在 secureCRT上通过 rz命令上传 jdk-8u201-linux-x64.tar.gz 到服务器，然后解压缩 jdk-8u201-linux-x64.tar.gz。
   tar zxvf jdk-8u201-linux-x64.tar.gz 

3, mv jdk1.8.0_201 /usr/local/java

4，vi /etc/profile
     新增以下内容	 

```	 
PATH=$PATH:/usr/local/java/bin
CLASSPATH=/usr/local/java/lib
```

5, source /etc/profile

6, java -version







