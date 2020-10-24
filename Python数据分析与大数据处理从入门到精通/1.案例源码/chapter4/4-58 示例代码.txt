# -*- coding: utf-8 -*-


def bubble_sort():
    tmp_data = data
    length = len(data)
    last_index = length - 1
    index1 = 0
    while index1 < length:
        index2 = 1
        while index2 <= last_index:
            if tmp_data[index2] < tmp_data[index2 - 1]:
                data[index2], data[index2 - 1] = data[index2 - 1], data[index2]
                print(data)
            index2 += 1

        index1 += 1
        print("第" + str(index1) + "趟排序完成")


if __name__ == '__main__':
    data = [10, -5, 7, 9, 2, 3, 8, -2, 0, -6]
    print("原始数据：", data)
    bubble_sort()
    print("排序后的数据：", data)

	