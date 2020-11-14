
### 抽取Excel的sheet数据

```
def extrad_g14_master_list_data():
    df = pd.read_excel('D:/g14.xlsx', sheet_name='G14 Master List',
                       usecols=['CUSTOMER_CATEGORY', 'G14 group ind', 'Total Risk Exposure\n风险暴露总和', 'CBS Group']
                       , skiprows=[1])
    df.to_excel('d:/g14_bak.xlsx')
```	
	