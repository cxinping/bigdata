
# 为Centos系统添加新的普通用户


使用root用户登录 CentOS 系统以后，打开一个终端，在终端中执行如下Shell命令，创建一个新的普通用户（比如，这里创建的用户名为 hadoop ）：

> sudo useradd -m hadoop -s /bin/bash

这条命令创建了可以登陆的 linziyu 用户，并使用 /bin/bash 作为 shell。
接着使用如下命令为这个新用户设置密码，请按系统提示输入两次密码

> sudo passwd hadoop
输入密码 12345678
可为 hadoop 用户增加管理员权限，方便部署，避免一些对新手来说比较棘手的权限问题

授权sudo权限，需要修改sudoers文件。 
a. 首先找到文件位置，示例中文件在/etc/sudoers位置。 
> whereis sudoers

b.强调内容 修改文件权限，一般文件默认为只读。 
> ls -l /etc/sudoers 

查看文件权限 
> chmod -v u+w /etc/sudoers 

修改文件权限为可编辑

c. 修改文件，在如下位置增加一行，保存退出。 
> vim /etc/sudoers 

进入文件编辑器 
文件内容改变如下： 
> root ALL=(ALL) ALL 已有行 
> hadoop ALL=(ALL) ALL 新增行

d. 记得将文件权限还原回只读。 
> ls -l /etc/sudoers 

查看文件权限 

> chmod -v u-w /etc/sudoers 

修改文件权限为只读

然后，把登录用户从root用户切换到linziyu用户
> su hadoop





#  **FinalShell** 

 

FinalShell 是一体化的的服务器,网络管理软件,不仅是ssh客户端,还是功能强大的开发,运维工具,充分满足开发,运维需求。 

```cpp
http://www.hostbuf.com/downloads/finalshell_install.exe

http://www.hostbuf.com/t/988.html


```

# FileZilla

FTP工具

# PyCharm安装

```
cd ~  #进入当前hadoop用户的主目录
sudo tar -zxvf ~/下载/pycharm-community-2016.3.2.tar.gz -C /usr/local  #把pycharm解压缩到/usr/local目录下
cd /usr/local
sudo mv pycharm-community-2016.3.2 pycharm  #重命名
sudo chown -R hadoop ./pycharm  #把pycharm目录权限赋予给当前登录Ubuntu系统的hadoop用户

```


