### 参考资料

https://openpyxl.readthedocs.io/en/stable/

https://zhuanlan.zhihu.com/p/62021331

https://www.osgeo.cn/openpyxl/usage.html

http://zetcode.com/python/openpyxl/

https://www.javatpoint.com/python-openpyxl

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