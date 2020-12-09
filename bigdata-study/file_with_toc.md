<a name="index">**Index**</a>
<a href="#0">调度</a>  
<a href="#1">apscheduler</a>  
<a href="#2">openpyxl</a>  
&emsp;<a href="#3">参考资料</a>  
&emsp;<a href="#4">安装 openpyxl</a>  
&emsp;<a href="#5">为 Excel 设置行高或者列宽</a>  
<a href="#6">设置行高</a>  
<a href="#7">设置列宽</a>  
&emsp;<a href="#8">创建excel,删除默认创建的第一个sheet页</a>  
<a href="#9">-*- coding: utf-8 -*-</a>  
<a href="#10">创建workbook对象，写入模式</a>  
<a href="#11">删除默认创建的一个sheet页</a>  
<a href="#12">1.创建sheet页</a>  
<a href="#13">构造 测试数据</a>  
<a href="#14">2.向工作表中 按行添加数据</a>  
&emsp;<a href="#15">设置激活工作表</a>  
&emsp;<a href="#16">获取工作表对象</a>  
&emsp;<a href="#17">styles样式处理</a>  
&emsp;<a href="#18">访问一个单元格</a>  
<a href="#19">访问A列4行的单元格，不存在则创建</a>  
<a href="#20">还有Worksheet.cell()方法，赋值（4，1）值为10</a>  
&emsp;<a href="#21">访问一个单元格</a>  
<a href="#22">A1~C2 2行3列所有的单元格</a>  
&emsp;<a href="#23">设置单行和一列的长和宽</a>  
<a href="#24">-*- coding: utf-8 -*-</a>  
<a href="#25">创建workbook对象，写入模式</a>  
<a href="#26">删除默认创建的一个sheet页</a>  
<a href="#27">创建sheet页</a>  
<a href="#28">调整列宽</a>  
<a href="#29">调整行高</a>  
&emsp;<a href="#30">设置数字格式</a>  
&emsp;<a href="#31">设置所有行和全部列的长和宽</a>  
<a href="#32">-*- coding: utf-8 -*-</a>  
<a href="#33">创建workbook对象，写入模式</a>  
<a href="#34">删除默认创建的一个sheet页</a>  
<a href="#35">创建sheet页</a>  
&emsp;<a href="#36">改变 worksheet 的背景色 </a>  
<a href="#37">-*- coding: utf-8 -*-</a>  
&emsp;<a href="#38">Merging cells</a>  
&emsp;<a href="#39">创建 sheet</a>  
<a href="#40">工作</a>  
&emsp;<a href="#41">抽取Excel的sheet数据</a>  
&emsp;<a href="#42">pandas将多个dataframe以多个sheet的形式保存到一个excel文件中</a>  
&emsp;<a href="#43">过滤数据</a>  
<a href="#44">Pandas</a>  
&emsp;&emsp;<a href="#45">访问 DataFrame</a>  
<a href="#46">print(df['b'][0] )</a>  
<a href="#47">方式2</a>  

# <a name="0">调度</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>
# <a name="1">apscheduler</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>


https://www.cnblogs.com/yueerwanwan0204/p/5480870.html

```
1， 假数据模型层

class ReportFakeData(db.Model):
    __tablename__ = 'lre_report_fake'
    id = db.Column(db.Integer,primary_key=True,autoincrement=True)
    input_label = db.Column(db.String(50),nullable=False)
    input_value = db.Column(db.String(50),nullable=False)
    fake_value = db.Column(db.String(50),nullable=False)
    created_time = db.Column(db.DateTime,default=datetime.now)#注册时间

    def to_json(self):
        return {
            'id': self.id,
            'input_label': self.input_label,
            'input_value': self.input_value,
            'fake_value': self.fake_value,
            'created_time': self.oper_time.strftime('%Y-%m-%d %H:%M:%S') if self.created_time != None else ''
        }
		
2， 异常修改

class BaseException(Exception):

    def __init__(self , message):
        self.__message = message

    def __str__(self):
        return self.__message


class ReportException(BaseException):
    pass


3, 创建 task 文件夹

创建 utils 文件夹
		
创建 enums 文件夹
		
		
4， IdGenerator
		
```

# <a name="2">openpyxl</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>

## <a name="3">参考资料</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>

WORKING WITH EXCEL SPREADSHEETS

https://automatetheboringstuff.com/2e/chapter13/

https://openpyxl.readthedocs.io/en/stable/

https://zhuanlan.zhihu.com/p/62021331

https://www.osgeo.cn/openpyxl/usage.html

http://zetcode.com/python/openpyxl/

https://www.javatpoint.com/python-openpyxl

## <a name="4">安装 openpyxl</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>

```
pip install openpyxl
```


## <a name="5">为 Excel 设置行高或者列宽</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>

openpyxl 的 Worksheet 对象拥有 row_dimensions 和 column_dimensions 属性，可分别用于控制行高和列宽。

```
import openpyxl
wb=openpyxl.Workbook()
sheet=wb.active

# <a name="6">设置行高</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>
sheet['A1']='行高被设置为 100'
sheet.row_dimensions[1].height=100

# <a name="7">设置列宽</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>
sheet['B2']='列宽被设置为 50'
sheet.column_dimensions['B'].width=50

wb.save('dimensions.xlsx')

```


## <a name="8">创建excel,删除默认创建的第一个sheet页</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>

```
# <a name="9">-*- coding: utf-8 -*-</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>

import openpyxl
# <a name="10">创建workbook对象，写入模式</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>
wb = openpyxl.Workbook()
# <a name="11">删除默认创建的一个sheet页</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>
ws = wb['Sheet']
wb.remove(ws)

# <a name="12">1.创建sheet页</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>
ws = wb.create_sheet(title='sheet-1')
# <a name="13">构造 测试数据</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>
row = ["A11","A12","A13"]
# <a name="14">2.向工作表中 按行添加数据</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>
ws.append(row)

wb.create_sheet(title='sheet-2')

filePath = "excel-demo11111.xlsx"
wb.save(filePath)
```

## <a name="15">设置激活工作表</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>
```
wb.active = 2   #设置active参数，即工作表索引值，以0位初始值
```

## <a name="16">获取工作表对象</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>
```
ws = wb["表名"]
```

## <a name="17">styles样式处理</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>

> https://www.jianshu.com/p/7af9a7c5b27d

```
from openpyxl.styles import Alignment

align = Alignment(horizontal='left',vertical='center',wrap_text=True)
ws.['D1'].alignment = align
```

horizontal代表水平方向，可以左对齐left，还有居中center和右对齐right，分散对齐distributed，跨列居中centerContinuous，两端对齐justify，填充fill，常规general

vertical代表垂直方向，可以居中center，还可以靠上top，靠下bottom，两端对齐justify，分散对齐distributed

另外还有自动换行：wrap_text，这是个布尔类型的参数，这个参数还可以写作wrapText

## <a name="18">访问一个单元格</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>
```
# <a name="19">访问A列4行的单元格，不存在则创建</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>
c = ws['A4']
# <a name="20">还有Worksheet.cell()方法，赋值（4，1）值为10</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>
d = ws.cell(row=4, column=1, value=10)
```

## <a name="21">访问一个单元格</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>
```
# <a name="22">A1~C2 2行3列所有的单元格</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>
cell_range = ws['A1':'C2']
```

## <a name="23">设置单行和一列的长和宽</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>

```
# <a name="24">-*- coding: utf-8 -*-</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>

import openpyxl
# <a name="25">创建workbook对象，写入模式</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>
wb = openpyxl.Workbook()
# <a name="26">删除默认创建的一个sheet页</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>
ws = wb['Sheet']
wb.remove(ws)

# <a name="27">创建sheet页</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>
ws = wb.create_sheet(title='sheet-1')
# <a name="28">调整列宽</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>
ws.column_dimensions['A'].width = 20.0
# <a name="29">调整行高</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>
ws.row_dimensions[1].height = 40

filePath = "excel-demo22222.xlsx"
wb.save(filePath)
```

## <a name="30">设置数字格式</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>

```
cell = sheet.cell(8, 2)
cell.value = 1955861.11
cell.number_format = '_(* #,##0.00_);_(* (#,##0.00);_(* "-"??_);_(@_)'
```

## <a name="31">设置所有行和全部列的长和宽</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>

```
# <a name="32">-*- coding: utf-8 -*-</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>

import openpyxl
from openpyxl.utils import get_column_letter
# <a name="33">创建workbook对象，写入模式</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>
wb = openpyxl.Workbook()
# <a name="34">删除默认创建的一个sheet页</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>
ws = wb['Sheet']
wb.remove(ws)

# <a name="35">创建sheet页</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>
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

## <a name="36">改变 worksheet 的背景色 </a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>

```
# <a name="37">-*- coding: utf-8 -*-</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>

from openpyxl import Workbook
import openpyxl

wb = openpyxl.load_workbook("example.xlsx")

ws = wb['b']
ws.sheet_properties.tabColor = "0072BA"
ws['A1'].value = "=VLOOKUP(a!A2,a!A1:B6,2,FALSE)"

wb.save("example.xlsx")
```

## <a name="38">Merging cells</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>

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

## <a name="39">创建 sheet</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>

```
import openpyxl
   
wb = openpyxl.load_workbook('d:\\g14_bak.xlsx')
#sheetxx = wb.create_sheet('china',0)
sheetxx = wb.create_sheet('sheet1' )
sheetxx['A1'] = 'juankuan'
sheetxx['E1'] = '中国'
sheetxx['F1'] = '2020-02-27-09-51-02'
wb.save('d:\\g14_bak.xlsx')
```









# <a name="40">工作</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>
## <a name="41">抽取Excel的sheet数据</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>

```
import pandas as pd

def extrad_g14_master_list_data():
    df = pd.read_excel('D:/g14.xlsx', sheet_name='G14 Master List',
                       usecols=['CUSTOMER_CATEGORY', 'G14 group ind', 'Total Risk Exposure\n风险暴露总和', 'CBS Group']
                       , skiprows=[1] )
    df.to_excel('d:/g14_bak.xlsx' ,sheet_name='G14 Master List' ,index=False)
```	
	
## <a name="42">pandas将多个dataframe以多个sheet的形式保存到一个excel文件中</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>

```	
import pandas as pd
from openpyxl import load_workbook

writer = pd.ExcelWriter('d:/g14_bak.xlsx', engin='openpyxl')
book = load_workbook(writer.path)
writer.book = book

data = {
	'性别': ['男', '女', '女', '男', '男'],
	'姓名': ['小明', '小红', '小芳', '大黑', '张三'],
	'年龄': [20, 21, 25, 24, 29]}
dataframe = pd.DataFrame(data, index=['one', 'two', 'three', 'four', 'five'],
				  columns=['姓名', '性别', '年龄', '职业'])
				  
dataframe.to_excel(excel_writer=writer, sheet_name="info1" , index=False)
writer.save()
writer.close()

```	
	
## <a name="43">过滤数据</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>

```	

import pandas as pd

def read_data():
    df = pd.read_excel('D:/g14_bak.xlsx', sheet_name='G14 Master List',
                       usecols=['CUSTOMER_CATEGORY', 'G14 group ind', 'Total Risk Exposure\n风险暴露总和', 'CBS Group']
                       , skiprows=[1])

    query_cond1 = ( df['CUSTOMER_CATEGORY'] == 'CORPORATE' ) | ( df['CUSTOMER_CATEGORY'] == 'GOV' ) | (df['CUSTOMER_CATEGORY'].isnull() )
    query_cond2 = df['G14 group ind'] == 'Y'
    query_cond3 = df[ 'Total Risk Exposure\n风险暴露总和'] != 0

    df2 = df[query_cond1 & query_cond1 & query_cond3 ]
    df3 = df2.groupby(['CBS Group'], as_index=False)['CBS Group'].agg({'cnt': 'count'})

    print(df3)

    rowsnum = df3.shape[1]
    colsnum = df3.shape[0]
	
	print('rowsnum={},colsnum={}'.format(rowsnum, colsnum)   )
	
	for index, row in df3.iterrows():
        print('index={}，CBS Group={},cnt={}'.format(index, row['CBS Group'], row['cnt'])  )
		
```	

# <a name="44">Pandas</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>

### <a name="45">访问 DataFrame</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>

```	

import numpy as np

data = np.arange(12).reshape(3,4)
df = pd.DataFrame(data)
df.columns = ['a' , 'b' , 'c' , 'd'  ]
rowNum = df.shape[0]
colNum = df.shape[1]


# <a name="46">print(df['b'][0] )</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>

#方式1
	for row in df.itertuples():
		print( getattr(row, 'a') , getattr(row, 'b') ,getattr(row, 'c') , getattr(row, 'd') )

# <a name="47">方式2</a><a style="float:right;text-decoration:none;" href="#index">[Top]</a>
	for x in range(rowNum):
		for y in range(colNum):
			print(df.iloc[x,y], end=' ')

		print()

```	



