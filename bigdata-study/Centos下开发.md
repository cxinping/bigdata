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







