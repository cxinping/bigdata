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



## Hadoop伪分布式配置
选择将 Hadoop 安装至 /usr/local/ 

```
sudo tar -zxf ~/下载/hadoop-3.1.4.tar.gz -C /usr/local    # 解压到/usr/local中

cd /usr/local/

sudo mv hadoop-3.1.4/ hadoop           # 将文件夹名改为hadoop
```

Hadoop 可以在单节点上以伪分布式的方式运行，Hadoop 进程以分离的 Java 进程来运行，节点既作为 NameNode 也作为 DataNode，同时，读取的是 HDFS 中的文件。

Hadoop 的配置文件位于 /usr/local/hadoop/etc/hadoop/ 中，伪分布式需要修改2个配置文件 core-site.xml 和 hdfs-site.xml 。Hadoop的配置文件是 xml 格式，每个配置以声明 property 的 name 和 value 的方式来实现。

修改配置文件 core-site.xml (通过 gedit 编辑会比较方便: gedit ./etc/hadoop/core-site.xml)，将当中的

```
<configuration>
</configuration>
```

修改为下面配置
```
<configuration>
	<property>
		<name>hadoop.tmp.dir</name>
		<value>file:/usr/local/hadoop/tmp</value>
		<description>Abase for other temporary directories.</description>
	</property>
	<property>
		<name>fs.defaultFS</name>
		<value>hdfs://localhost:9000</value>
	</property>
</configuration>
```

同样的，修改配置文件 hdfs-site.xml

```
<configuration>
	<property>
		<name>dfs.replication</name>
		<value>1</value>
	</property>
	<property>
		<name>dfs.namenode.name.dir</name>
		<value>file:/usr/local/hadoop/tmp/dfs/name</value>
	</property>
	<property>
		<name>dfs.datanode.data.dir</name>
		<value>file:/usr/local/hadoop/tmp/dfs/data</value>
	</property>
</configuration>
```

Hadoop配置文件说明

Hadoop 的运行方式是由配置文件决定的（运行 Hadoop 时会读取配置文件），因此如果需要从伪分布式模式切换回非分布式模式，需要删除 core-site.xml 中的配置项。

此外，伪分布式虽然只需要配置 fs.defaultFS 和 dfs.replication 就可以运行（官方教程如此），不过若没有配置 hadoop.tmp.dir 参数，则默认使用的临时目录为 /tmp/hadoo-hadoop，而这个目录在重启时有可能被系统清理掉，导致必须重新执行 format 才行。所以我们进行了设置，同时也指定 dfs.namenode.name.dir 和 dfs.datanode.data.dir，否则在接下来的步骤中可能会出错。


配置完成后，执行 NameNode 的格式化
```
cd /usr/local/hadoop

./bin/hdfs namenode -format
```
输入大写字母 "Y"


接着开启 NameNode 和 DataNode 守护进程。
```
cd /usr/local/hadoop
./sbin/start-dfs.sh  #start-dfs.sh是个完整的可执行文件，中间没有空格
```

出现以下错误，
```
[root@localhost sbin]# start-dfs.sh
Starting namenodes on [localhost]
ERROR: Attempting to operate on hdfs namenode as root
ERROR: but there is no HDFS_NAMENODE_USER defined. Aborting operation.
```

问题1：Attempting to operate on hdfs namenode as root

在/usr/local/hadoop/sbin路径下，将start-dfs.sh，stop-dfs.sh两个文件顶部添加以下参数

```
#!/usr/bin/env bash
HDFS_DATANODE_USER=root
HADOOP_SECURE_DN_USER=hdfs
HDFS_NAMENODE_USER=root
HDFS_SECONDARYNAMENODE_USER=root
```

start-yarn.sh，stop-yarn.sh顶部也需添加以下
```
#!/usr/bin/env bash
YARN_RESOURCEMANAGER_USER=root
HADOOP_SECURE_DN_USER=yarn
YARN_NODEMANAGER_USER=root
```

修改后重启 ./start-dfs.sh



问题2：
出现错误 ERROR: JAVA_HOME is not set and could not be found.到hadoop的安装目录修改配置文件“/usr/local/hadoop/etc/hadoop/hadoop-env.sh”，在里面找到“export JAVA_HOME=${JAVA_HOME}”这行，然后，把它修改成JAVA安装路径的具体地址，比如，“export JAVA_HOME=/usr/local/java”，然后，再次启动Hadoop。


## 设置环境变量

vi /etc/profile

```
export HADOOP_HOME=/usr/local/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
```
使配置文件生效
```
source /etc/profile
```



## 启动成功Hadoop
成功启动后，可以访问 Web 界面 http://localhost:9870 查看 NameNode 和 Datanode 信息，还可以在线查看 HDFS 中的文件。

> http://localhost:9870

> http://192.168.11.10:9870



启动Hadoop

> cd /usr/local/hadoop
>
> ./sbin/start-dfs.sh



1, 启动ResourceManager

> cd /usr/local/hadoop
> sbin/yarn-daemon.sh start resourcemanager

2,启动NodeManager

> cd /usr/local/hadoop
> sbin/yarn-daemon.sh start nodemanager





测试HDFS是否能正常创建目录。

> hdfs dfs -mkdir /input


## 文件管理

1,创建目录
> hdfs dfs -mkdir /test

2, 上传文件到系统

> hdfs dfs -put /usr/local/hadoop/etc/hadoop/core-site.xml /test

3, 查看目录

> hdfs dfs -ls /test


























