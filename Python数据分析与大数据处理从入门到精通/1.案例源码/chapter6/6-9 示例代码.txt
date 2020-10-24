# -*- coding: UTF-8 -*-

def get_list():
    count = 0
    while True:
        print("------本次循环开始，count初始值为：", count)
        outer = yield count
        print("<---生成器从外部接收到的数据：", outer)
        count += 1
        print("------本次循环结束，count值为：", count)
        print()


gen = get_list()
val = next(gen)
print("--->外部调用next从生成器获取到的值：", val)
print()
val = gen.send(20)
print("--->外部从生成器获取到的值：", val)
gen.close()

