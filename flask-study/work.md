
### 抽取Excel的sheet数据

```
def extrad_g14_master_list_data():
    df = pd.read_excel('D:/g14.xlsx', sheet_name='G14 Master List',
                       usecols=['CUSTOMER_CATEGORY', 'G14 group ind', 'Total Risk Exposure\n风险暴露总和', 'CBS Group']
                       , skiprows=[1])
    df.to_excel('d:/g14_bak.xlsx' ,sheet_name='G14 Master List')
```	
	
### pandas将多个dataframe以多个sheet的形式保存到一个excel文件中

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
	