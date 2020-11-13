### 参考资料

WORKING WITH EXCEL SPREADSHEETS

https://automatetheboringstuff.com/2e/chapter13/

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
wb.active = 2   #设置active参数，即工作表索引值，以0位初始值
```

### 获取工作表对象
```
ws = wb["表名"]
```

### styles样式处理

https://www.jianshu.com/p/7af9a7c5b27d

```
from openpyxl.styles import Alignment

align = Alignment(horizontal='left',vertical='center',wrap_text=True)
ws.['D1'].alignment = align
```

horizontal代表水平方向，可以左对齐left，还有居中center和右对齐right，分散对齐distributed，跨列居中centerContinuous，两端对齐justify，填充fill，常规general

vertical代表垂直方向，可以居中center，还可以靠上top，靠下bottom，两端对齐justify，分散对齐distributed

另外还有自动换行：wrap_text，这是个布尔类型的参数，这个参数还可以写作wrapText

### 访问一个单元格
```
# 访问A列4行的单元格，不存在则创建
c = ws['A4']
# 还有Worksheet.cell()方法，赋值（4，1）值为10
d = ws.cell(row=4, column=1, value=10)
```

### 访问一个单元格
```
# A1~C2 2行3列所有的单元格
cell_range = ws['A1':'C2']
```

### 设置单行和一列的长和宽

```
# -*- coding: utf-8 -*-

import openpyxl
# 创建workbook对象，写入模式
wb = openpyxl.Workbook()
# 删除默认创建的一个sheet页
ws = wb['Sheet']
wb.remove(ws)

# 创建sheet页
ws = wb.create_sheet(title='sheet-1')
# 调整列宽
ws.column_dimensions['A'].width = 20.0
# 调整行高
ws.row_dimensions[1].height = 40

filePath = "excel-demo22222.xlsx"
wb.save(filePath)
```

### 设置数字格式

```
cell = sheet.cell(8, 2)
cell.value = 1955861.11
cell.number_format = '_(* #,##0.00_);_(* (#,##0.00);_(* "-"??_);_(@_)'
```

### 设置所有行和全部列的长和宽

```
# -*- coding: utf-8 -*-

import openpyxl
from openpyxl.utils import get_column_letter
# 创建workbook对象，写入模式
wb = openpyxl.Workbook()
# 删除默认创建的一个sheet页
ws = wb['Sheet']
wb.remove(ws)

# 创建sheet页
ws = wb.create_sheet(title='sheet-1')

width = 20
height = 10

print("row:", ws.max_row, "column:", ws.max_column)
for i in range(1, ws.max_row+1):
    ws.row_dimensions[i].height = height
for i in range(1, ws.max_column+1):
    ws.column_dimensions[get_column_letter(i)].width = width

filePath = "excel-demo22222.xlsx"
wb.save(filePath)
```

### 改变 worksheet 的背景色 

```
# -*- coding: utf-8 -*-

from openpyxl import Workbook
import openpyxl

wb = openpyxl.load_workbook("example.xlsx")

ws = wb['b']
ws.sheet_properties.tabColor = "0072BA"
ws['A1'].value = "=VLOOKUP(a!A2,a!A1:B6,2,FALSE)"

wb.save("example.xlsx")
```

### Merging cells

```
from openpyxl import Workbook
from openpyxl.styles import Alignment

book = Workbook()
sheet = book.active

sheet.merge_cells('A1:B2')

cell = sheet.cell(row=1, column=1)
cell.value = 'Sunny day'
cell.alignment = Alignment(horizontal='center', vertical='center')

book.save('merging.xlsx')
```





