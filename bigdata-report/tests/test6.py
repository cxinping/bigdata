# -*- coding: utf-8 -*-
def list_of_groups(list_info, per_list_len):
    '''
    :param list_info:   列表
    :param per_list_len:  每个小列表的长度
    :return:
    '''
    list_of_group = zip(*(iter(list_info),) * per_list_len)
    end_list = [list(i) for i in list_of_group]  # i is a tuple
    count = len(list_info) % per_list_len
    end_list.append(list_info[-count:]) if count != 0 else end_list
    return end_list


if __name__ == '__main__':
    list_info = ['name zhangsan', 'age 10', 'sex man', 'name lisi', 'age 11', 'sex women', 'aaaa', 'bbb', 'ccc']
    ret = list_of_groups(list_info, 5)
    print(ret)
