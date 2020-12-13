# -*- coding: utf-8 -*-
from fdfs_client.client import Fdfs_client, get_tracker_conf

def upload_demo():
    #tracker_path = get_tracker_conf(r'D:\work_software\python_workspace\FastDFSDemo\client.conf')
    tracker_path = get_tracker_conf(r'.\client.conf')
    client = Fdfs_client(tracker_path)
    result = client.upload_by_filename(r'd:\test\car1.jpg')
    print(result)

    if result.get('Status') != 'Upload successed.':
        raise Exception('上传文件到FastDFS失败')
    filename = result.get('Remote file_id')
    print('filename={}'.format(filename))

def download_demo():
    tracker_path = get_tracker_conf(r'D:\work_software\python_workspace\FastDFSDemo\client.conf')
    client = Fdfs_client(tracker_path)
    result = client.download_to_file(local_filename=r'd:\test\car2.jpg',remote_file_id=b'group1/M00/00/00/wKgLCl_SO76AGaoJAABDY48Ib9w180.jpg')
    print(result)

def delete_demo():
    tracker_path = get_tracker_conf(r'D:\work_software\python_workspace\FastDFSDemo\client.conf')
    client = Fdfs_client(tracker_path)
    result = client.delete_file(remote_file_id=b'group1/M00/00/00/wKgLCl_SO76AGaoJAABDY48Ib9w180.jpg')
    print(result)

if __name__ == '__main__':
    upload_demo()
