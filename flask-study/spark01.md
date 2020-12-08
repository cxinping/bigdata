厦门大学林子雨老师的课程

> http://dblab.xmu.edu.cn/post/spark

> https://zhuanlan.zhihu.com/p/103818677

> http://dblab.xmu.edu.cn/blog/2373/

Linux：实现Hadoop集群Master无密码登录（SSH）各个子节点

> https://www.cnblogs.com/yy3b2007com/p/5878859.html

Spark Streaming 集成 Kafka 总结

> https://colobu.com/2015/01/05/kafka-spark-streaming-integration-summary/


# Hadoop安装教程_单机/伪分布式配置_Hadoop2.6.0(2.7.1)/Ubuntu14.04(16.04)

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

选择将 Hadoop 安装至 /usr/local/ 

```
sudo tar -zxf ~/下载/hadoop-3.1.4.tar.gz -C /usr/local    # 解压到/usr/local中
cd /usr/local/
sudo mv ./hadoop-2.6.0/ ./hadoop            # 将文件夹名改为hadoop
sudo chown -R hadoop ./hadoop       # 修改文件权限

```


# 安装Python3 环境

安装Python3前的库环境
```
$ yum install gcc patch libffi-devel python-devel  zlib-devel bzip2-devel openssl-devel ncurses-devel sqlite-devel readline-devel tk-devel gdbm-devel db4-devel libpcap-devel xz-devel -y

```

把Python-3.6.4.tar上传到CentOS的/software文件夹下，解压文件
```
$ cd /software
$ tar –xvf Python-3.6.4.tar
```

进入解压后的文件夹
```
$ cd Python-3.6.4/
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





