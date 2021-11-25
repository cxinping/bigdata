# -*- coding: utf-8 -*-
import sys
sys.setdefaultencoding('utf-8')

dict_file ="/you_filed_algos/app/report/algorithm/userdict.txt"
rst_dict_file ="/you_filed_algos/app/report/algorithm/userdict_rst.txt"

fo = open(dict_file, "r")
content = ""
while True:
    line = fo.readline()
    if not line:
        break
    temp = line.strip().split("\t")
    content += temp[0]+ " " + temp[1] +"\r\n"

    wo = open(rst_dict_file ,"w")
    wo.write(content)
