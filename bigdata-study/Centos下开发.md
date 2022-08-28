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

conda create --name report python=3.7
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

删除虚拟环境

```
conda remove -n your_env_name --all

conda remove -n report --all
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



# 安装 Xmanager 



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
pip install --download /packs pandas

pip install --download /data/workspace/report/deploy/linux_packs Django
```

​	 或 　                        

```
pip install --download /data/workspace/report/deploy/linux_packs -r requirements.txt
```

```crystal
pip install --download /tmp/offline_packages -r requirements.txt
```

存放requirements.txt列出的所有包

如果出现异常 pip no such option: --download，需要对pip指定版本， 将pip版本设置为20.2.4 

```
 pip install pip==20.2.4
```

pip升级

```
 pip install --upgrade pip  

pip freeze > requirements.txt

python -m pip download /data/workspace/report/deploy/linux_packs -r requirements.txt

python -m pip wheel --wheel-dir /data/workspace/report/deploy/linux_packs -r requirements.txt
```



## 离线下载安装包



```
cd /data/workspace/report/deploy
cd D:\BI\report\deploy

pip freeze > requirements.txt  
```



Linux离线下载whl安装包

```
python -m pip download --destination-directory /data/workspace/report/deploy/linux_packs -r requirements.txt

```

Windows离线下载whl安装包

```
D:\work_software\anaconda\envs\report\python.exe -m pip install --upgrade pip


py -m pip download --destination-directory D:/BI/report/deploy/win_packs -r requirements.txt

pip install --download D:/BI/report/deploy/win_packs -r requirements.txt

```



安装单个模块

```
pip  download  Pillow   -d .  --trusted-host pypi.douban.com -i http://pypi.douban.com/simple

根据需要下载单个模块
pip download Django -d D:/BI/report/deploy/win_packs  --trusted-host pypi.douban.com -i http://pypi.douban.com/simple

pip download PyMySQL -d D:/BI/report/deploy/win_packs  --trusted-host pypi.douban.com -i http://pypi.douban.com/simple

pip download openpyxl -d D:/BI/report/deploy/win_packs  --trusted-host pypi.douban.com -i http://pypi.douban.com/simple

pip download Pillow -d D:/BI/report/deploy/win_packs  --trusted-host pypi.douban.com -i http://pypi.douban.com/simple

pip download python-docx -d D:/BI/report/deploy/win_packs  --trusted-host pypi.douban.com -i http://pypi.douban.com/simple

pip download docx2pdf -d D:/BI/report/deploy/win_packs  --trusted-host pypi.douban.com -i http://pypi.douban.com/simple
```



安装单个模块

```
pip install --no-index --find-links="/tmp/tranferred_packages" <package> 
 
pip install --no-index --find-links="D:/BI/report/deploy/win_packs" Django
 
pip install --no-index --find-links="D:/BI/report/deploy/win_packs" PyMySQL
 
pip install --no-index --find-links="D:/BI/report/deploy/win_packs" openpyxl

pip install --no-index --find-links="D:/BI/report/deploy/win_packs" Pillow

pip install --no-index --find-links="D:/BI/report/deploy/win_packs" python-docx

pip install --no-index --find-links="D:/BI/report/deploy/win_packs" docx2pdf
  
```











## 离线安装安装包

Linux安装下载whl安装包

```
python -m pip install --no-index --find-links=DIR -r requirements.txt

cd /data/workspace/report/deploy

python -m pip install --no-index --find-links=/data/workspace/report/deploy/linux_packs -r requirements.txt
```



## Installing from local packages

In some cases, you may want to install from local packages only, with no traffic to PyPI.

First, download the archives that fulfill your requirements:

 **Unix/macOS** 

```
python -m pip download --destination-directory DIR -r requirements.txt
```

 **Windows** 

```
py -m pip download --destination-directory DIR -r requirements.txt
```

 Note that `pip download` will look in your wheel cache first, before trying to download from PyPI. If you’ve never installed your requirements before, you won’t have a wheel cache for those items. In that case, if some of your requirements don’t come as wheels from PyPI, and you want wheels, then run this instead: 

 **Unix/macOS** 

```
python -m pip wheel --wheel-dir DIR -r requirements.txt
```

**Windows** 

```
py -m pip wheel --wheel-dir DIR -r requirements.txt
```

 Then, to install from local only, you’ll be using [--find-links](https://pip.pypa.io/en/stable/cli/pip_install/#install-find-links) and [--no-index](https://pip.pypa.io/en/stable/cli/pip_install/#install-no-index) like so: 

 **Unix/macOS** 

```
python -m pip install --no-index --find-links=DIR -r requirements.txt
```

 **Windows** 

```
py -m pip install --no-index --find-links=DIR -r requirements.txt
```





参考资料

```
https://blog.csdn.net/qq_34218221/article/details/90243121

https://pip.pypa.io/en/stable/user_guide/#installing-from-local-packages

http://t.zoukankan.com/wt11-p-6216508.html
```

