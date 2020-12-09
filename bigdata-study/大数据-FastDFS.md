
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
fdfs_download_file  /etc/fdfs/client.conf group1/M00/00/00/ wKgLCl9EnDSAUGD1AAa9gM70jnk407.jpg
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













































