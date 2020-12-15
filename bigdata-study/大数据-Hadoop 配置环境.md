# Hadoop伪分布式部署

是指相关守护进行都独立运行，只是运行在同一太计算机上，使用HDFS来存储数据，一般用来模拟一个小规模的集群。




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
配置好JDK8后，在命令行输入 java -version会返回如下信息
```
[root@localhost local]# java -version
java version "1.8.0_271"
Java(TM) SE Runtime Environment (build 1.8.0_271-b09)
Java HotSpot(TM) 64-Bit Server VM (build 25.271-b09, mixed mode)
```


## 安装SSH、配置SSH无密码登陆

1)集群、单节点模式都需要用到 SSH 登陆（类似于远程登陆，你可以登录某台 Linux 主机，并且在上面运行命令），Ubuntu 默认已安装了 SSH client，此外还需要安装 SSH server：

```
sudo yum install openssh-server 
```

首先退出刚才的 ssh，就回到了我们原先的终端窗口，然后利用 ssh-keygen 生成密钥，并将密钥加入到授权中


```
exit                           # 退出刚才的 ssh localhost
cd ~/.ssh/                     # 若没有该目录，请先执行一次ssh localhost
ssh-keygen -t rsa              # 会有提示，都按回车就可以
# cat ./id_rsa.pub >> ./authorized_keys  # 加入授权
 
```
输入ssh-keygen -t rsa ，然后会遇到三次让输入的时候，第一次直接回车，第二次和第三次分别是：让输入密码和确认密码，我们这里是要实现无密码登录。所以以上三次输入都直接回车，不设置什么密码，也就是空密码登录。


2)之后在/home/hadoop/下会产生一个.ssh的文件夹, 用ls查看

```
cd /home/hadoop/.ssh
cd ~/.ssh
```

3)将公钥追加到authorized_keys文件中
```
cp ~/.ssh/id_rsa.pub ~/.ssh/authorized_keys
```

此时再用 ssh localhost 命令，无需输入密码就可以直接登陆了，如下图所示
```
ssh localhost
```












