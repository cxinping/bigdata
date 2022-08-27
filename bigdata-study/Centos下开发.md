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

​	安装Python3.7的虚拟环境，别名是superset

```
conda create --name superset python=3.7
```
得到如下信息
```
#
# To activate this environment, use
#
#     $ conda activate superset
#
# To deactivate an active environment, use
#
#     $ conda deactivate
```

​	进入虚拟环境

```
conda activate superset
```

如果抛出异常
```
CommandNotFoundError: Your shell has not been properly configured to use 'conda activate'.
To initialize your shell, run

    $ conda init <SHELL_NAME>

Currently supported shells are:
  - bash
  - fish
  - tcsh
  - xonsh
  - zsh
  - powershell

See 'conda init --help' for more information and options.

IMPORTANT: You may need to close and restart your shell after running 'conda init'.
```

重新进入虚拟环境

```
source activate
```

重新退出虚拟环境

```
source deactivate
```



# 安装PyCharm

​	从https://www.jetbrains.com/pycharm/ 下载PyCharm的Linux安装包 pycharm-community-2022.2.1.tar.gz



```
mv pycharm-community-2022.2.1.tar.gz /usr/local

tar zxvf pycharm-community-2022.2.1.tar.gz 

mv pycharm-community-2022.2.1 pycharm

cd /pycharm/bin

启动PyCharm
sh pycharm.sh
```



# 安装 xmanager 



打开xmanager ,  设置安全隧道

![](https://images.cnblogs.com/cnblogs_com/wangshuo1/1613306/o_220827030525_%E5%BE%AE%E4%BF%A1%E5%9B%BE%E7%89%87_20220827110427.png)



​	然后打开 xshell, 进入到 /usr/local/pycharm ，输入命令

```
sh pycharm.sh
```



# django开发

# 安装django

```
pip install django==3.2
```



## 创建模块

```mel
python manage.py startapp article

python manage.py startapp export
```











# 开发技巧

## 清理yum数据源



```
yum clean
yum makecache
```



##  Python导出离线安装包 



​	 下载指定的包到指定文件夹。      

```
pip list #查看安装的包  
```

​    	将已经通过pip安装的包的名称记录到 requirements.txt文件中      

```
pip freeze > requirements.txt  
```

​		创建存放安装包的目录：

```
mkdir /packs        
```

​      存放一个pandas包      

```
 pip install  --download  /packs  pandas
```

​	 或 　                        

```
pip install --download /data/workspace/report/deploy/linux_packs -r requirements.txt
```

​	存放requirements.txt列出的所有包



如果出现异常 pip no such option: --download，需要对pip指定版本， 将pip版本设置为20.2.4 

```
 pip install pip==20.2.4
```

pip升级

```
 pip install --upgrade pip  
```



```
pip freeze > requirements.txt
```







参考资料

```
http://t.zoukankan.com/wt11-p-6216508.html
```

