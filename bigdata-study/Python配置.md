pycharm设置默认编码为utf-8

第一步在我们的电脑上打开pycharm，点击file->settings
第二步进去settings界面之后，点击Editor->File Encodings 
第三步将Global Encoding和project Encoding的编码设置为utf-8，点击下拉框可以进行设置，如下图所示
第五步我们也可以设置属性文件“Default Encoding for properties files”的编码为utf-8


# -*- coding: utf-8 -*-  

VS Code 改变默认文字编码 为utf-8
File(文件)->Preferences(首选项)->Usersettings(设置)
搜索 encod  或者 encoding ，然后修改为想要的编码格式。


安装pip.ini

在C:\Users\swang50\ 目录下新建pip文件夹，在pip文件加下，创建pip.ini文件，内容如下。


Ansible playbooks
https://docs.ansible.com/ansible/latest/user_guide/playbooks.html


def create_random_number(length=10):
    """ 生成len位的随机数 """

    raw = ""
    range1 = range(58, 65)  # between 0~9 and A~Z
    range2 = range(91, 97)  # between A~Z and a~z

    i = 0
    while i < length:
        seed = random.randint(48, 122)
        if ((seed in range1) or (seed in range2)):
            continue
        raw += chr(seed)
        i += 1

    return raw
