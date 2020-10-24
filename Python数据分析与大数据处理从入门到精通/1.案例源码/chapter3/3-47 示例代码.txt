# -*- coding: UTF-8 -*-

data_list = ["hello", [1, 2, 3, 4, 5, 6, 7, 8], ("abc",), 123, 5.888]
print("遍历列表与嵌套：")
for i in data_list:
    print("data_listd的直接元素：", i)
    if isinstance(i, list):
        print("遍历内部列表：", end="\t")
        for j in i:
            print(j, end="\t")

        print("")

print("")
data_dic = {"key1": "hello", "key2": "world"}
print("遍历字典：")
for key, val in data_dic.items():
    print("key:", key, "value:", val)

	
	