
# FastDFS 分布式文件管理系统



## FastDFS介绍

FastDFS是一个开源的分布式文件系统，她对文件进行管理，功能包括：文件存储、文件同步、文件访问（文件上传、文件下载）等，解决了大容量存储和负载均衡的问题。特别适合以文件为载体的在线服务，如相册网站、视频网站等等。

FastDFS服务端有两个角色：跟踪器（tracker）和存储节点（storage）。跟踪器主要做调度工作，在访问上起负载均衡的作用。

存储节点存储文件，完成文件管理的所有功能：存储、同步和提供存取接口，FastDFS同时对文件的meta data进行管理。所谓文件的meta data就是文件的相关属性，以键值对（key value pair）方式表示，如：width=1024，其中的key为width，value为1024。文件meta data是文件属性列表，可以包含多个键值对。

![dfs](.\images\dfs.jpg)



为了支持大容量，存储节点（服务器）采用了分卷（或分组）的组织方式。存储系统由一个或多个卷组成，卷与卷之间的文件是相互独立的，所有卷  的文件容量累加就是整个存储系统中的文件容量。一个卷可以由一台或多台存储服务器组成，一个卷下的存储服务器中的文件都是相同的，卷中的多台存储服务器起 到了冗余备份和负载均衡的作用。

在卷中增加服务器时，同步已有的文件由系统自动完成，同步完成后，系统自动将新增服务器切换到线上提供服务。

当存储空间不足或即将耗尽时，可以动态添加卷。只需要增加一台或多台服务器，并将它们配置为一个新的卷，这样就扩大了存储系统的容量。
 FastDFS中的文件标识分为两个部分：卷名和文件名，二者缺一不可。

 **FastDFS file upload** 

![dfs2](.\images\dfs2.jpg)





 上传文件交互过程：

1. client询问tracker上传到的storage，不需要附加参数。

2. tracker返回一台可用的storage。

3. client直接和storage通讯完成文件上传。

   

 **FastDFS file download** 

![dfs3](.\images\dfs3.jpg)



下载文件交互过程：

1. client询问tracker下载文件的storage，参数为文件标识（卷名和文件名）；

2. tracker返回一台可用的storage；

3. client直接和storage通讯完成文件下载。

需要说明的是，client为使用FastDFS服务的调用方，client也应该是一台服务器，它对tracker和storage的调用均为服务器间的调用。


### 参考

官方网站：https://github.com/happyfish100/

配置文档：https://github.com/happyfish100/fastdfs/wiki/

参考资料：https://www.oschina.net/question/tag/fastdfs

Java客户端：https://github.com/happyfish100/fastdfs-client-java

### 术语

fastDFS

```
FastDFS是一款开源的轻量级分布式文件系统纯C实现，支持Linux、FreeBSD等UNIX系统类google FS，不是通用的文件系统，只能通过专有API访问，目前提供了C、Java和PHP API为互联网应用量身定做，解决大容量文件存储问题，追求高性能和高扩展性FastDFS可以看做是基于文件的key value pair存储系统，称作分布式文件存储服务更为合适。
------ 来自官网介绍
```

tracker-server:
```
跟踪服务器， 主要做调度工作， 起负载均衡的作用。 在内存中记录集群中所有存储组和存储服务器的状态信息， 是客户端和数据服务器交互的枢纽。 相比GFS中的master更为精简， 不记录文件索引信息， 占用的内存量很少。
```

storage-server:
```
存储服务器（ 又称：存储节点或数据服务器） ， 文件和文件属性（ metadata） 都保存到存储服务器上。 Storage server直接利用OS的文件系统调用管理文件。
```

group
```
组， 也可称为卷。 同组内服务器上的文件是完全相同的 ，同一组内的storage server之间是对等的， 文件上传、 删除等操作可以在任意一台storage server上进行 。
```

meta data
```
meta data：文件相关属性，键值对（ Key Value Pair） 方式，如：width=1024,heigth=768 。
```



## 安装FastDFS



### 编译环境



``` 
yum install git gcc gcc-c++ make automake autoconf libtool pcre pcre-devel zlib zlib-devel openssl-devel wget vim -y
 

yum install gcc-c++

yum install -y pcre pcre-devel

yum install -y zlib zlib-devel

yum -y install pcre pcre-devel zlib zlib-devel openssl openssl-devel
```



### 磁盘目录

| 说明         | 位置           |
| ------------ | -------------- |
| 所有安装包   | /usr/local/src |
| 数据存储位置 | /home/dfs/     |

 

创建数据存储目录
```
$ mkdir /home/dfs 
```


切换到安装目录准备下载安装包
```
$ cd /usr/local/src 
```


### 安装libfastcommon
```
$ cd /usr/local/src 

$ git clone https://github.com/happyfish100/libfastcommon.git --depth 1

$ cd libfastcommon/

$ ./make.sh && ./make.sh install #编译安装
```


### 安装FastDFS

```
rm -f /etc/fdfs

$ cd /usr/local/src 

$ git clone https://github.com/happyfish100/fastdfs.git --depth 1

$ cd fastdfs/

$ ./make.sh && ./make.sh install #编译安装
```

配置文件准备
```
$ cp /etc/fdfs/tracker.conf.sample /etc/fdfs/tracker.conf
$ cp /etc/fdfs/storage.conf.sample /etc/fdfs/storage.conf
$ cp /etc/fdfs/client.conf.sample /etc/fdfs/client.conf #客户端文件，测试用
$ cp /usr/local/src/fastdfs/conf/http.conf /etc/fdfs/ #供nginx访问使用
$ cp /usr/local/src/fastdfs/conf/mime.types /etc/fdfs/ #供nginx访问使用

$ cd /etc/fdfs/
$ rm -rf client.conf.sample storage.conf.sample tracker.conf.sample
```

### 安装fastdfs-nginx-module

```
$ cd /usr/local/src 

$ git clone https://github.com/happyfish100/fastdfs-nginx-module.git --depth 1

$ cp /usr/local/src/fastdfs-nginx-module/src/mod_fastdfs.conf /etc/fdfs
```

### 安装nginx

```
$ cd /usr/local/src 

$ wget http://nginx.org/download/nginx-1.15.4.tar.gz #下载nginx压缩包

$ tar -zxvf nginx-1.15.4.tar.gz #解压

$ cd nginx-1.15.4/

# 添加fastdfs-nginx-module模块
$ ./configure --add-module=/usr/local/src/fastdfs-nginx-module/src/ 

$ make && make install #编译安装
```

## 单机部署

### tracker配置
服务器ip为 192.168.11.10

```
$ vi /etc/fdfs/tracker.conf
```

需要修改的内容如下
```
port=22122                   # tracker服务器端口（默认22122,一般不修改）
base_path=/home/dfs/tracker  # 存储日志和数据的根目录
```


在tracker.conf文件里的store_lookup=0 轮询向storage存储文件，在fastdfs及群里使用



生成tracker存储日志和数据的根目录

```
mkdir -p /home/dfs/tracker
```



启动tracker

```
$ /etc/init.d/fdfs_trackerd start 
```



查看 FastDFS Tracker 是否已成功启动 ，22122端口正在被监听，则算是Tracker服务安装成功。

```
$ netstat -unltp|grep fdfs

$ ps -ef | grep fdfs
```

### storage配置
```
$ vim /etc/fdfs/storage.conf
```

需要修改的内容如下
```
port=23000  # storage服务端口（默认23000,一般不修改）
base_path=/home/dfs/storage  # 数据和日志文件存储根目录
store_path0=/home/dfs/storage  # 第一个存储目录
tracker_server=192.168.11.10:22122  # tracker服务器IP和端口
http.server_port=8888  # http访问文件的端口(默认8888,看情况修改,和nginx中保持一致)


```



创建存储路径

```
mkdir -p /home/dfs/storage
```



启动 Storage

```
$ /etc/init.d/fdfs_storaged start
```



查看 Storage 是否成功启动，23000 端口正在被监听，就算 Storage 启动成功。

```
$ netstat -unltp|grep fdfs

$ ps -ef | grep fdfs_storaged
```



查看Storage和Tracker是否在通信：

```
/usr/bin/fdfs_monitor  /etc/fdfs/storage.conf
```


### client测试

```
$ vim /etc/fdfs/client.conf
```

需要修改的内容如下
```
base_path=/home/dfs/tracker   #tracker服务器文件路径

tracker_server=192.168.11.10:22122    #tracker服务器IP和端口
```

保存后测试,返回ID表示成功 如：group1/M00/00/00/xx.tar.gz

#### 上传文件

测试上传文件
```
$ fdfs_upload_file /etc/fdfs/client.conf /usr/local/src/nginx-1.15.4.tar.gz
```



例如: 上传文件

```
[root@localhost fdfs]# fdfs_upload_file /etc/fdfs/client.conf /usr/local/src/nginx-1.15.4.tar.gz
group1/M00/00/00/wKgLCl9Eh0GAUrxzAA-itrfn0m4.tar.gz

```


成功之后会返回图片的路径

```
group1/M00/00/00/wKgLCl9Eh0GAUrxzAA-itrfn0m4.tar.gz
```

组名：group1 
磁盘：M00 
目录：00/00 
文件名称：wKgLCl9Eh0GAUrxzAA-itrfn0m4.tar.gz


例如：上传图片
```
$ fdfs_upload_file /etc/fdfs/client.conf /usr/local/src/fenjing.jpg
group1/M00/00/00/wKgLCl9EnDSAUGD1AAa9gM70jnk407.jpg
```

组名：group1 
磁盘：M00 
目录：00/00 
文件名称：wKgLCl9EnDSAUGD1AAa9gM70jnk407.jpg



去上传路径查看，是否上传成功。

```
$ cd /home/dfs/storage
```



#### 下载文件

```
fdfs_download_file /etc/fdfs/client.conf group1/M00/00/00/ wKgLCl9EnDSAUGD1AAa9gM70jnk407.jpg a.jpg
```



#### 删除文件

```
fdfs_delete_file  /etc/fdfs/client.conf group1/M00/00/00/ wKgLCl9EnDSAUGD1AAa9gM70jnk407.jpg
```



## 配置nginx访问  

```
vim /etc/fdfs/mod_fastdfs.conf
```



需要修改的内容如下

```
tracker_server=192.168.11.10:22122  #tracker服务器IP和端口
url_have_group_name=true
store_path0=/home/dfs/storage  
```

配置nginx.config
```
vim /usr/local/nginx/conf/nginx.conf
```



添加如下配置
```
server {
    listen       8888;    ## 该端口为storage.conf中的http.server_port相同
    server_name  localhost;
    location ~/group[0-9]/ {
        ngx_fastdfs_module;
    }
    error_page   500 502 503 504  /50x.html;
    location = /50x.html {
    root   html;
    }
}
```



测试下载，用外部浏览器访问刚才已传过的nginx安装包,引用返回的ID

http://192.168.11.10:8888/group1/M00/00/00/wKgLCl9EnDSAUGD1AAa9gM70jnk407.jpg
http://192.168.11.10:8888/group1/M00/00/00/wKgLCl9E19qALeueAAa9gM70jnk705.jpg

常用Nginx操作
```
$ /usr/local/nginx/sbin/nginx -V

$ /usr/local/nginx/sbin/nginx -s reload

$ /usr/local/nginx/sbin/nginx -s stop

$ /usr/local/nginx/sbin/nginx -c /usr/local/nginx/conf/nginx.conf
```


##启动
### 关闭防火墙


不关闭防火墙的话无法正常使用FastDFS
```
$ systemctl stop firewalld.service #停止firewall
$ systemctl disable firewalld.service #禁止firewall开机启动
```


### tracker
启动tracker服务
```
$ /etc/init.d/fdfs_trackerd start 
```
重启动tracker服务
```
$ /etc/init.d/fdfs_trackerd restart 
```
停止tracker服务
```
$ /etc/init.d/fdfs_trackerd stop 
```
自启动tracker服务
```
$ chkconfig fdfs_trackerd on 
```

### storage
启动storage服务
```
$ /etc/init.d/fdfs_storaged start 
```
重动storage服务
```
$ /etc/init.d/fdfs_storaged restart 
```
停止动storage服务
```
$ /etc/init.d/fdfs_storaged stop 
```
自启动storage服务
```
$ chkconfig fdfs_storaged on 
```

### nginx
启动nginx
```
$ /usr/local/nginx/sbin/nginx 

$ /usr/local/nginx/sbin/nginx -c /usr/local/nginx/conf/nginx.conf
```

重启nginx
```
$ /usr/local/nginx/sbin/nginx -s reload
```

停止nginx
```
$ /usr/local/nginx/sbin/nginx -s stop
```

# Python操作FastDFS


## 安装py3Fdfs
Python3与FastDFS交互使用py3Fdfs, 安装FastDFS客户端模块 py3Fdfs
```
pip install py3Fdfs
```



## FastDFS配置文件



client.conf 参考例子

```
# connect timeout in seconds
# default value is 30s
# Note: in the intranet network (LAN), 2 seconds is enough.
connect_timeout = 5

# network timeout in seconds
# default value is 30s
network_timeout = 60

# the base path to store log files
base_path = d:/fdfs_log

# tracker_server can ocur more than once for multi tracker servers.
# the value format of tracker_server is "HOST:PORT",
#   the HOST can be hostname or ip address,
#   and the HOST can be dual IPs or hostnames seperated by comma,
#   the dual IPS must be an inner (intranet) IP and an outer (extranet) IP,
#   or two different types of inner (intranet) IPs.
#   for example: 192.168.2.100,122.244.141.46:22122
#   another eg.: 192.168.1.10,172.17.4.21:22122

tracker_server = 192.168.11.10:22122
#tracker_server = 192.168.0.197:22122

#standard log level as syslog, case insensitive, value list:
### emerg for emergency
### alert
### crit for critical
### error
### warn for warning
### notice
### info
### debug
log_level = info

# if use connection pool
# default value is false
# since V4.05
use_connection_pool = false

# connections whose the idle time exceeds this time will be closed
# unit: second
# default value is 3600
# since V4.05
connection_pool_max_idle_time = 3600

# if load FastDFS parameters from tracker server
# since V4.05
# default value is false
load_fdfs_parameters_from_tracker = false

# if use storage ID instead of IP address
# same as tracker.conf
# valid only when load_fdfs_parameters_from_tracker is false
# default value is false
# since V4.05
use_storage_id = false

# specify storage ids filename, can use relative or absolute path
# same as tracker.conf
# valid only when load_fdfs_parameters_from_tracker is false
# since V4.05
storage_ids_filename = storage_ids.conf


#HTTP settings
http.tracker_server_port = 80

#use "#include" directive to include HTTP other settiongs
##include http.conf
```

## 上传文件
```
# -*- coding: utf-8 -*-
from fdfs_client.client import Fdfs_client, get_tracker_conf

def upload_demo():
    tracker_path = get_tracker_conf(r'D:\work_software\python_workspace\FastDFSDemo\client.conf')
    client = Fdfs_client(tracker_path)
    result = client.upload_by_filename(r'd:\test\car1.jpg')
    print(result)

    if result.get('Status') != 'Upload successed.':
        raise Exception('上传文件到FastDFS失败')
    filename = result.get('Remote file_id')
    print('filename={}'.format(filename))
```
运行代码得到如下返回值

```
{'Group name': b'group1', 'Remote file_id': b'group1/M00/00/00/wKgLCl_SO76AGaoJAABDY48Ib9w180.jpg', 'Status': 'Upload successed.', 'Local file name': 'd:\\test\\car1.jpg', 'Uploaded size': '16.85KB', 'Storage IP': b'192.168.11.10'}
```

可以通过Nginx访问上传的图片

> http://192.168.11.10:8888/group1/M00/00/00/wKgLCl_SO76AGaoJAABDY48Ib9w180.jpg


## 下载文件

```
from fdfs_client.client import Fdfs_client, get_tracker_conf

def download_demo():
    tracker_path = get_tracker_conf(r'D:\work_software\python_workspace\FastDFSDemo\client.conf')
    client = Fdfs_client(tracker_path)
    result = client.download_to_file(local_filename=r'd:\test\car2.jpg',remote_file_id=b'group1/M00/00/00/wKgLCl_SO76AGaoJAABDY48Ib9w180.jpg')
    print(result)
	
download_demo()
```


运行代码得到如下返回值
```
{'Remote file_id': b'group1/M00/00/00/wKgLCl_SO76AGaoJAABDY48Ib9w180.jpg', 'Content': 'd:\\test\\car2.jpg', 'Download size': '16.85KB', 'Storage IP': b'192.168.11.10'}
```

把 remote_file_id为b'group1/M00/00/00/wKgLCl_SO76AGaoJAABDY48Ib9w180.jpg'的图片保存到了本地硬盘 d:/test/car2.jpg下


## 删除文件

```
from fdfs_client.client import Fdfs_client, get_tracker_conf

def delete_demo():
    tracker_path = get_tracker_conf(r'D:\work_software\python_workspace\FastDFSDemo\client.conf')
    client = Fdfs_client(tracker_path)
    result = client.delete_file(remote_file_id=b'group1/M00/00/00/wKgLCl_SO76AGaoJAABDY48Ib9w180.jpg')
    print(result)
	
delete_demo()
```

运行代码得到如下返回值
```
('Delete file successed.', b'group1/M00/00/00/wKgLCl_SO76AGaoJAABDY48Ib9w180.jpg', b'192.168.11.10')

```

去服务器的 /home/dfs/storage 路径下查看是否成功删除上传到FastDFS的图片
```
cd /home/dfs/storage
```










