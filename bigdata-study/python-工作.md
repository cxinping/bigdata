

# 调度

## 参考资料

Flask WTF
```
http://codingdict.com/article/4881

```




## markdown-toc

> https://blog.csdn.net/weixin_33695082/article/details/91367266

> https://github.com/ekalinin/github-markdown-toc.go/releases



## 案例

```
1， cache_utils.py

from werkzeug.contrib.cache import SimpleCache

from apps.model1 import User

cache = SimpleCache()
def getUserById(userId):
    key = "user:"+str(userId)
    rv = cache.get(key)
    if rv is None:
        user = User.query.filter_by(id=userId).first()
        if user:
            rv = user
            cache.set(key, rv, timeout=5 * 60)
        return rv
    else:
        print("===============hit on==============="+str(userId))
        return  rv
        }
		
2， 异常修改

class BaseException(Exception):

    def __init__(self , message):
        self.__message = message

    def __str__(self):
        return self.__message


class ReportException(BaseException):
    pass
		
3, 创建系统模型

# 定义lre的日志数据模型
class Oper_Log(db.Model):
       __tablename__='sys_oper_log'

       id = db.Column(db.Integer, primary_key=True, autoincrement=True)
       module_name = db.Column(db.String(100) )
       method_name = db.Column(db.String(200) )
       line_number = db.Column(db.INTEGER)
       level = db.Column(db.String(50))
       message = db.Column(db.String(5000) )
       oper_time = db.Column(db.DateTime )

       def __init__(self, module_name, method_name , line_number, level, message , oper_time = datetime.now() ):
              self.module_name = module_name
              self.method_name = method_name
              self.line_number = line_number
              self.level = level
              self.message = message
              self.oper_time = oper_time

       def to_json(self):
           return {
               'id': self.id,
               'module_name': self.module_name,
               'method_name': self.method_name,
               'line_number' : self.line_number, 
               'level' : self.level,
               'message': self.message,
               'oper_time': self.oper_time.strftime('%Y-%m-%d %H:%M:%S') if self.oper_time !=None else ''
           }
		   
5， 获取模块名，方法名

def get_module_name():
    module_name = str(os.path.basename(__file__)).split('.')[0]
    funcName = sys._getframe().f_back.f_code.co_name  # 获取调用函数名
    lineNumber = sys._getframe().f_back.f_lineno  # 获取行号
	
6， IdGenerator
		
import uuid

def generate_uuid():
    uuid4 = str(uuid.uuid4())
    uuid4_str = ''.join(uuid4.split('-'))
    return uuid4_str

def generate_uuid2():
    import time
    time_id = str(int(time.time() * 1000)) + str(int(time.clock() * 1000000))
    return time_id
	
```

7, 验证时间有效性

```

import time

def is_valid_date(str_date):
    '''判断是否是一个有效的日期字符串'''
    try:
        time.strptime(str_date, "%Y%m%d")
        return True
    except Exception as err:
        return False

8， 通过某个关键字排序一个字典列表

rows = [
    {'fname': 'Brian', 'lname': 'Jones', 'uid': 1003},
    {'fname': 'David', 'lname': 'Beazley', 'uid': 1002},
    {'fname': 'John', 'lname': 'Cleese', 'uid': 1001},
    {'fname': 'Big', 'lname': 'Jones', 'uid': 1004}
]

from operator import itemgetter

rows_by_uid = sorted(rows, key=itemgetter('uid'))

9，读取配置文件

在config下有一个config.ini配置文件
#  定义config分组
[config]
platformName=Android
appPackage=com.romwe
appActivity=com.romwe.SplashActivity
 
#  定义cmd分组
[cmd]
viewPhone=adb devices
startServer=adb start-server
stopServer=adb kill-server
install=adb install aaa.apk
id=1
weight=12.1
isChoice=True
 
#  定义log分组
[log]
log_error=true

在test_config.py中编写读取配置文件的脚本代码

import configparser
 
#  实例化configParser对象
config = configparser.ConfigParser()
# -read读取ini文件
config.read('C:\\Users\\songlihui\\PycharmProjects\\AutoTest_02\\config\\config.ini', encoding='GB18030')
# -sections得到所有的section，并以列表的形式返回
print('sections:' , ' ' , config.sections())
 
# -options(section)得到该section的所有option
print('options:' ,' ' , config.options('config'))
 
# -items（section）得到该section的所有键值对
print('items:' ,' ' ,config.items('cmd'))
 
# -get(section,option)得到section中option的值，返回为string类型
print('get:' ,' ' , config.get('cmd', 'startserver'))
 
# -getint(section,option)得到section中的option的值，返回为int类型
print('getint:' ,' ' ,config.getint('cmd', 'id'))
print('getfloat:' ,' ' , config.getfloat('cmd', 'weight'))
print('getboolean:' ,'  ', config.getboolean('cmd', 'isChoice'))
"""
首先得到配置文件的所有分组，然后根据分组逐一展示所有
"""
for sections in config.sections():
    for items in config.items(sections):
        print(items)
```


# openpyxl

## 参考资料

WORKING WITH EXCEL SPREADSHEETS

https://automatetheboringstuff.com/2e/chapter13/

https://openpyxl.readthedocs.io/en/stable/

https://zhuanlan.zhihu.com/p/62021331

https://www.osgeo.cn/openpyxl/usage.html

http://zetcode.com/python/openpyxl/

https://www.javatpoint.com/python-openpyxl

## 安装 openpyxl

```
pip install openpyxl
```


## 为 Excel 设置行高或者列宽

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


## 创建excel,删除默认创建的第一个sheet页

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

## 设置激活工作表
```
wb.active = 2   #设置active参数，即工作表索引值，以0位初始值
```

## 获取工作表对象
```
ws = wb["表名"]
```

## styles样式处理

> https://www.jianshu.com/p/7af9a7c5b27d

```
from openpyxl.styles import Alignment

align = Alignment(horizontal='left',vertical='center',wrap_text=True)
ws.['D1'].alignment = align
```

horizontal代表水平方向，可以左对齐left，还有居中center和右对齐right，分散对齐distributed，跨列居中centerContinuous，两端对齐justify，填充fill，常规general

vertical代表垂直方向，可以居中center，还可以靠上top，靠下bottom，两端对齐justify，分散对齐distributed

另外还有自动换行：wrap_text，这是个布尔类型的参数，这个参数还可以写作wrapText

## 访问一个单元格
```
# 访问A列4行的单元格，不存在则创建
c = ws['A4']
# 还有Worksheet.cell()方法，赋值（4，1）值为10
d = ws.cell(row=4, column=1, value=10)
```

## 访问多个单元格
```
# A1~C2 2行3列所有的单元格
cell_range = ws['A1':'C2']
```

## 设置单行和一列的长和宽

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

## 设置数字格式

```
cell = sheet.cell(8, 2)
cell.value = 1955861.11
cell.number_format = '_(* #,##0.00_);_(* (#,##0.00);_(* "-"??_);_(@_)'
```

## Converting Between Column Letters and Numbers

```
>>> import openpyxl
>>> from openpyxl.utils import get_column_letter, column_index_from_string
>>> get_column_letter(1) # Translate column 1 to a letter.
'A'
>>> get_column_letter(2)
'B'
>>> get_column_letter(27)
'AA'
>>> get_column_letter(900)
'AHP'
>>> wb = openpyxl.load_workbook('example.xlsx')
>>> sheet = wb['Sheet1']
>>> get_column_letter(sheet.max_column)
'C'
>>> column_index_from_string('A') # Get A's number.
1
>>> column_index_from_string('AA')
27

```

## 将第1列改为百分比

```
ws.cell(row=1,column=1).number_format = '0.00%'
ws.cell(row=2,column=1).number_format = '0.00%'
ws.cell(row=3,column=1).number_format = '0.00%'
ws.cell(row=4,column=1).number_format = '0.00%'
```

## 将第2列改为小数点后两位

```
ws.cell(row=1,column=2).number_format = '0.00'
ws.cell(row=2,column=2).number_format = '0.00'
ws.cell(row=3,column=2).number_format = '0.00'
ws.cell(row=4,column=2).number_format = '0.00'
```

## 将第3列改为带货币符号

```
ws.cell(row=1,column=3).number_format = '"￥"#,###' 
ws.cell(row=2,column=3).number_format = '"￥"#,###' 
ws.cell(row=3,column=3).number_format = '"￥"#,###' 
ws.cell(row=4,column=3).number_format = '"￥"#,###'
```

## 将第4列改为年月日时分秒格式
```
ws.cell(row=1,column=4).number_format = 'yyyy-MM-dd HH:mm:ss'
ws.cell(row=2,column=4).number_format = 'yyyy-MM-dd HH:mm:ss'
ws.cell(row=3,column=4).number_format = 'yyyy-MM-dd HH:mm:ss'
ws.cell(row=4,column=4).number_format = 'yyyy-MM-dd HH:mm:ss'

```

## border

```
from openpyxl.styles.borders import Border, Side
from openpyxl import Workbook

thin_border = Border(left=Side(style='thin'), 
                     right=Side(style='thin'), 
                     top=Side(style='thin'), 
                     bottom=Side(style='thin'))

wb = Workbook()
ws = wb.get_active_sheet()
# property cell.border should be used instead of cell.style.border
ws.cell(row=3, column=2).border = thin_border
wb.save('border_test.xlsx')
```

## 复制工作表

```
# 拷贝工作表
ws2 = wb.copy_worksheet(ws)

# 将复制后的工作表名称用红色填充
ws2.sheet_properties.tabColor = 'FF0000'

# 全部完成后，进行保存。
wb.save('exists_book.xlsx')
```

## 设置所有行和全部列的长和宽

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

## 改变 worksheet 的背景色 

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

## 合并单元格 Merging cells

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

## 创建 sheet

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

## 合并单元格，换行

```
import openpyxl

wb = openpyxl.load_workbook('d:/g14_bak.xlsx')
from openpyxl.styles import Alignment

ws = wb['sheet1']

ws.merge_cells('A2:D3')

str2 = '1\n2\n3'
ws['A2'].value = str2
ws['A2'].alignment = Alignment(wrapText=True)

wb.save('d:/g14_bak.xlsx')

```

## 操作单元格

```
cell = ws['A4'] #获取第4行第A列的单元格

ws['A4'] = 4 #给第4行第A列的单元格赋值为4

ws.cell(row=4, column=2, value=10) #给第4行第2列的单元格赋值为10

```

获取区域内的单元格，如下

```
cell_range = ws['A1':'C2']  #获取A1-C2内的区域

colC = ws['C']  #获取第C列
col_range = ws['C:D']  #获取第C-D列
row10 = ws[10]  #获取第10列
row_range = ws[5:10]  #获取第5-10列

```

获取单元格的值

```
cellValue = ws.cell(row=i, column=j).value

```
## 获取sheets

```
import openpyxl

report_file = "C:/codes2/g14_quarterly_report_template.xlsx"
report_file2 = "C:/codes2/g14_quarterly_report_template2.xlsx"
wb = openpyxl.load_workbook(report_file)

print(wb.sheetnames)
g1401_sheet = wb['G1401']
g1402_sheet = wb['G1402']
wb.remove(g1401_sheet)
wb.remove(g1402_sheet)
wb.active = 3

wb.save(report_file2)

print('--- ok ---')

```

## 单元格填充颜色

```
from openpyxl import Workbook
from openpyxl.styles import  PatternFill

#2-新建一个工作簿
wb = Workbook()
ws = wb.active

#随便赋个值
d4 = ws['D4']
d4 = '43'

#3-设置样式，并且加载到对应单元格
fill = PatternFill("solid", fgColor="1874CD")
d4.fill = fill

#保存文件
wb.save('test.xlsx')
```

## 读取excel中公式的结果值

```
wb=openpyxl.load_workbook("文件路径",data_only=True)
```
例子1

```
import openpyxl

def test(from_report):
    wb = openpyxl.load_workbook(from_report, data_only=True)

    sheet = wb['Summary Control']
    cell_value = sheet['C2'].value
    print(cell_value)
    cell_value = sheet['C3'].value
    print(cell_value)

from win32com.client import Dispatch

def just_open(filename):
    xlApp = Dispatch("Excel.Application")
    xlApp.Visible = False
    xlBook = xlApp.Workbooks.Open(filename)
    xlBook.Save()
    xlBook.Close()
```

例子2
```
import openpyxl

def test(from_report):
    print('* test')
    wb = openpyxl.load_workbook(from_report, data_only=True, keep_vba=True,)
    sheet = wb['Summary Control']
    cell_value = sheet['C2'].value
    print(cell_value)
    cell_value = sheet['C3'].value
    print(cell_value)

import xlwings
def save_open_excel(report_path):
    print('* save_open_excel')
    excel_app = xlwings.App(visible=False)
    excel_book = excel_app.books.open(report_path)
    excel_book.save()
    excel_book.close()
    excel_app.quit()

if __name__ == '__main__':   
    report_file2 = r"C:\codes2\bmo-lre\bmolre\templates\report\daily.xlsx"

    save_open_excel(report_file2)
    test(report_file2)
```

# 工作

## 精确到小数点后6位

```
from decimal import Decimal
rt = Decimal(1 / 0.916857).quantize(Decimal('0.000000'))
print(rt)
```

## 抽取Excel的sheet数据

```
import pandas as pd

def extrad_g14_master_list_data():
    df = pd.read_excel('D:/g14.xlsx', sheet_name='G14 Master List',
                       usecols=['CUSTOMER_CATEGORY', 'G14 group ind', 'Total Risk Exposure\n风险暴露总和', 'CBS Group']
                       , skiprows=[1] )
    df.to_excel('d:/g14_bak.xlsx' ,sheet_name='G14 Master List' ,index=False)
```

## pandas将多个dataframe以多个sheet的形式保存到一个excel文件中

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


## 过滤数据

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

# Pandas

## 访问 DataFrame


```	
import numpy as np

data = np.arange(12).reshape(3,4)
df = pd.DataFrame(data)
df.columns = ['a' , 'b' , 'c' , 'd'  ]
rowNum = df.shape[0]
colNum = df.shape[1]


print(df['b'][0] )

###方式1
for row in df.itertuples():
	print( getattr(row, 'a') , getattr(row, 'b') ,getattr(row, 'c') , getattr(row, 'd') )

###方式2
for x in range(rowNum):
	for y in range(colNum):
		print(df.iloc[x,y], end=' ')

	print()

```

## Pandas新增一列并按条件赋值

```	
import pandas as pd
import numpy as np

df = pd.DataFrame({'amount': [100, 200, 300, 400, 500], 'list': ['', '商品1', '商品2', '', '商品3']})
df['new'] = np.where(df['list'] == '', 0, df['amount'])

print(df)
```

## pandas实现两个dataframe数据的合并：按行和按列

1、按行合并（df1,df2上下拼接），axis=0可省略。

```	
pd.concat([df1,df2],axis=0)

**例子：**

df1= pd.DataFrame(0,columns=["a","b"],index=range(5))  
df2= pd.DataFrame(1,columns=["a","b"],index=range(3))  
pd.concat([df1,df2],axis=0)

```	


2、按列合并（df1,df2左右拼接）
```
pd.concat([df1,df2],axis=1)

**例子：**

df1= pd.DataFrame(0,columns=["a","b"],index=range(5))  
df2= pd.DataFrame(1,columns=["a","b"],index=range(3))  
pd.concat([df1,df2],axis=1)


```

3 根据某列，将两个 dataframe 合并

```
import pandas as pd
import numpy as np
df1 = pd.DataFrame(np.array([['a', 5, 9], ['b', 4, 61], ['c', 24, 9]]),
                   columns = ['name', 'attr11', 'attr12'])
df2 = pd.DataFrame(np.array([['a', 5, 19], ['b', 14, 16], ['c', 4, 9]]),
                   columns = ['name', 'attr21', 'attr22'])
df3 = pd.DataFrame(np.array([['a', 15, 49], ['b', 4, 36], ['c', 14, 9]]),
                   columns = ['name', 'attr31', 'attr32'])
print(pd.merge(pd.merge(df1, df2, on = 'name'), df3, on = 'name'))

```

# 请求数据

```
def get_url_content(url, form , max_try_number=5):
    try_num = 5
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36'
    }

    while True:
        try:
            response = requests.post(url=url, data=form, headers=headers, timeout=120)
            return response.json()
        except Exception as err:
            print(url, '抓取数据报错',str(err))
            try_num = try_num + 1
            if try_num >= max_try_number:
                print('尝试失败次数超过5次，放弃尝试!')
                return None

```

