# 安装Anaconda

## 在CentOS下安装Anaconda	

​	打开Anaconda官网 https://www.anaconda.com/products/distribution#linux 下载Linux版本的Anaconda,比如 Anaconda3-2022.05-Linux-x86_64.sh

​	在Linux里面.sh文件是可执行的脚本文件，需要使用命令bash来进行安装，输入命令

```
bash Anaconda3-2022.05-Linux-x86_64.sh
```

​	安装在   **/usr/local/anaconda** 目录下

​	修改/etc/profile文件，在文件的末尾加上以下代码

```
export PATH=$PATH:/usr/local/anaconda/bin
```

​	最后重新载入配置文件，输入 source /etc/profile



## 创建虚拟环境

```
conda create --name superset python=3.7

conda activate superset
```

 









