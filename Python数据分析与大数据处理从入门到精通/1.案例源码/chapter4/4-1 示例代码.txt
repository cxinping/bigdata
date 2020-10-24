# -*- coding: UTF-8 -*-


def test():
    """
    这个是一个没有方法体的函数，使用pass占位符代替代码块
    :return:
    """
    pass

def add(item):
    """
    该函数有参数item,并使用return返回函数处理结果
    :param item:
    :return:
    """
    return 5 + item

def show(item):
    """
    该函数有方法体，但是没有指定返回值，返回None
    :param item: 
    :return: 
    """
    5 + item

test()
print("调用add方法的返回值：", add(5))
print("调用show方法的返回值：", show(5))


