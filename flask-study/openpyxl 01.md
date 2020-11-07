### 参考资料

https://openpyxl.readthedocs.io/en/stable/

https://zhuanlan.zhihu.com/p/62021331

https://www.osgeo.cn/openpyxl/usage.html

http://zetcode.com/python/openpyxl/

https://www.javatpoint.com/python-openpyxl

### 安装 openpyxl

```
pip install openpyxl
```


### 为 Excel 设置行高或者列宽

openpyxl 的 Worksheet 对象拥有 row_dimensions 和 column_dimensions 属性，可分别用于控制行高和列宽。

```
import openpyxl
wb=openpyxl.Workbook()
sheet=wb.active

# 设置行高
sheet['A1']='行高被设置为 100'
sheet.row_dimensions[1].height=100

# 设置列宽
sheet['B2']='列宽被设置为 50'
sheet.column_dimensions['B'].width=50

wb.save('dimensions.xlsx')

```


### 创建excel,删除默认创建的第一个sheet页

```
# -*- coding: utf-8 -*-

import openpyxl
# 创建workbook对象，写入模式
wb = openpyxl.Workbook()
# 删除默认创建的一个sheet页
ws = wb['Sheet']
wb.remove(ws)

# 1.创建sheet页
ws = wb.create_sheet(title='sheet-1')
# 构造 测试数据
row = ["A11","A12","A13"]
# 2.向工作表中 按行添加数据
ws.append(row)

wb.create_sheet(title='sheet-2')

filePath = "excel-demo11111.xlsx"
wb.save(filePath)
```

### 设置激活工作表
```
wb.active = 2   #设置active参数，即工作表索引值
```






