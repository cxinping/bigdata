厦门大学林子雨老师的课程

> http://dblab.xmu.edu.cn/post/spark

> https://zhuanlan.zhihu.com/p/103818677

> http://dblab.xmu.edu.cn/blog/2373/

Linux：实现Hadoop集群Master无密码登录（SSH）各个子节点

> https://www.cnblogs.com/yy3b2007com/p/5878859.html

Spark Streaming 集成 Kafka 总结

> https://colobu.com/2015/01/05/kafka-spark-streaming-integration-summary/



# Hadoop安装教程_单机/伪分布式配置_

> http://dblab.xmu.edu.cn/blog/install-hadoop/

### 创建hadoop用户

```
sudo useradd -m hadoop -s /bin/bash

```

接着使用如下命令设置密码，可简单设置为 hadoop，按提示输入两次密码

```
sudo passwd hadoop
```

### 更新 yum

```
yum update
```

切换账号，从root切换到hadoop账号
```
su hadoop

```


### 安装SSH、配置SSH无密码登陆

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








## 安装 Hadoop 2


### 设置JDK

```
tar -zxvf jdk-8u271-linux-x64.tar.gz -C /usr/local

cd /usr/local

mv jdk1.8.0_271/ jdk8
```

设置环境变量

vi /etc/profile
```
export JAVA_HOME=/usr/local/jdk8
export CLASSPATH=$CLASSPATH:$JAVA_HOME/lib:.:
export HADOOP_HOME=/usr/local/hadoop
export PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

```

激活配置
```
source /etc/profile
```

### Hadoop伪分布式配置
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

出现以下错误，问题1：
```
[root@localhost sbin]# start-dfs.sh
Starting namenodes on [localhost]
ERROR: Attempting to operate on hdfs namenode as root
ERROR: but there is no HDFS_NAMENODE_USER defined. Aborting operation.
```

在/hadoop/sbin路径下：
将start-dfs.sh，stop-dfs.sh两个文件顶部添加以下参数

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
出现错误 ERROR: JAVA_HOME is not set and could not be found.到hadoop的安装目录修改配置文件“/usr/local/hadoop/etc/hadoop/hadoop-env.sh”，在里面找到“export JAVA_HOME=${JAVA_HOME}”这行，然后，把它修改成JAVA安装路径的具体地址，比如，“export JAVA_HOME=/usr/lib/jvm/default-java”，然后，再次启动Hadoop。


### 启动成功Hadoop
成功启动后，可以访问 Web 界面 http://localhost:9870 查看 NameNode 和 Datanode 信息，还可以在线查看 HDFS 中的文件。

http://localhost:9870



# PySpark

## 安装Python3 环境

安装Python3前的库环境
```
$ yum install gcc patch libffi-devel python-devel  zlib-devel bzip2-devel openssl-devel ncurses-devel sqlite-devel readline-devel tk-devel gdbm-devel db4-devel libpcap-devel xz-devel -y
```

把Python-3.9.1.tgz上传到CentOS的/software文件夹下，解压文件

```
$ cd /software
$ tar –xvf Python-3.9.1.tgz
```

进入解压后的文件夹

```
$ cd Python-3.9.1.tgz
```

编译安装Python3的默认安装路径是/usr/local，如果要改成其他目录可以在编译(make)前使用configure命令后面追加参数“-prefix=/usr/local/python”来完成修改，指定Python3的安装目录为/usr/local/python。
```
$ ./configure -prefix=/usr/local/python
```
编译Python源码
```
$ make 
```
执行安装
```
$ make install
```
至此已经在CentOS7系统中成功安装了Python3,还需要把Python3的配置信息添加到Linux的环境变量PATH中，修改/etc/profile下的内容
```
$ vi /etc/profile
```
添加以下内容在文件末尾，然后保存文件，退出到命令行。
```
export PATH=/usr/local/python/bin:.:$PATH
```
最后，激活 /etc/profile文件
```
$ source /etc/profile
```



## Linux下pycharm的安装

```
mkdir /usr/local/pycharm

tar -zxvf pycharm-community-2020.2.3.tar.gz -C /usr/local 
```

进入你所解压的目录中找到解压文件，即/usr/local/pycharm 
```
cd /usr/local 

mv pycharm-community-2020.2.3/ pycharm

cd /usr/local/pycharm/bin

运行 pycharm
./pycharm.sh
```

把启动Pycharm的脚本放到 /etc/profile里

$ vi /etc/profile
```
export PATH=/usr/local/pycharm/bin:.:$PATH
```
最后，激活 /etc/profile文件
```
$ source /etc/profile
```



## 安装虚拟环境

装虚拟环境软件包
```
$ pip3 install virtualenv

$ pip3 install -i https://pypi.doubanio.com/simple/ virtualenv
```

在 projects 文件夹下创建一个独立的虚拟环境，用于支持该项目，以后MyPlatformPlus启动时将会从这个虚拟环境启动；创建MyPlatformPlus文件夹及创建虚拟环境的代码如下；
```
$ mkdir /root/projects  #创建项目目录

$ cd /root/projects     #进入目录                 

$ virtualenv ENV        #创建一个虚拟环境，虚拟环境的名字为ENV
```

激活虚拟环境
```
$ source ENV/bin/activate
```

# 安装 PyCharm

http://dblab.xmu.edu.cn/blog/hadoop-build-project-using-eclipse/



# 安装Spark

步骤1：解压文件

> tar -zxvf spark-2.4.0-bin-hadoop2.7.tgz -C /usr/local/

步骤2：重命名Spark文件夹

> cd /usr/local/

> mv spark-2.4.0-bin-hadoop2.7/ spark


步骤3：重命名spark-env文件

> cd /usr/local/spark/conf

> mv spark-env.sh.template spark-env.sh

步骤4：配置环境变量，修改文件

> vi /etc/profile

添加Spark路径

```
export JAVA_HOME=/usr/local/java
export SPARK_HOME=/usr/local/spark
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.7-src.zip:$PYTHONPATH
export PYSPARK_PYTHON=python3
export PATH=$SPARK_HOME/bin:$PATH
```

步骤4：使修改生效
> source /etc/profile



## 启动Spark

> cd $SPARK_HOME
> ./sbin/start-all.sh

访问 http://192.168.11.10:8080

![spark1](.\images\spark1.jpg)



pip3 install -i https://pypi.tuna.tsinghua.edu.cn/simple pipenv



## 安装PySpark

在Spark安装中pyspark和spark.egg-info文件夹，将其复制到Python3安装目录下(/usr/local/python/lib/python3.9/site-packages)。








































