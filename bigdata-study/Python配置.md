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
    




def __sftp_put_file(local_file):
    host = app.config[CFG_SFTP_HOST] if CFG_SFTP_HOST in app.config else log.warning(WARN_MSG_MISSING_CONFIG, CFG_SFTP_HOST)
    port = app.config[CFG_SFTP_PORT] if CFG_SFTP_PORT in app.config else log.warning(WARN_MSG_MISSING_CONFIG, CFG_SFTP_PORT)
    username = app.config[CFG_SFTP_USER] if CFG_SFTP_USER in app.config else log.warning(WARN_MSG_MISSING_CONFIG, CFG_SFTP_USER)
    private_key_path = app.config[CFG_SFTP_PRIVATE_PATH] if CFG_SFTP_PRIVATE_PATH in app.config else log.warning(WARN_MSG_MISSING_CONFIG, CFG_SFTP_PRIVATE_PATH)
    remote_dirs = app.config[CFG_SFTP_PUT_DIRS] if CFG_SFTP_PUT_DIRS in app.config else log.warning(WARN_MSG_MISSING_CONFIG, CFG_SFTP_PUT_DIRS)

    log.info("sftp put %s to %s", local_file, remote_dirs)
    with SFTPClient(host, port, username, private_key_path) as sftp_client:
        for remote_dir in remote_dirs:
            sftp_client.put(local_file, '/'.join((remote_dir, os.path.basename(local_file))))

import paramiko

class SFTPClient:
    @log_entry_exit
    def __init__(self, host, port, username, private_key_path):
        self.host = host
        self.port = port
        self.username = username
        self.private_key_path = private_key_path

    def __enter__(self):
        self.transport = paramiko.Transport((self.host, self.port))
        with open(self.private_key_path) as private_key_file:
            private_key = paramiko.RSAKey.from_private_key(private_key_file)
        self.transport.connect(username=self.username, pkey=private_key)
        self.sftp_client = paramiko.SFTPClient.from_transport(self.transport)
        return self.sftp_client

    def __exit__(self, exc_type, exc_value, traceback):
        if self.sftp_client is not None:
            self.sftp_client.close()
        if self.transport is not None:
            self.transport.close()

        if exc_type is not None:
            tb.print_exception(exc_type, exc_value, traceback)

        return True


