# -*- coding: utf-8 -*-
str1 ='中国建设银行南昌江铜支行'
idx = str1.index('银行')
print(idx)
print(str1[idx+2:])


if 'a' in ['a' , 'b']:
    print('ok')

print('*******' * 30)

s1 ='3700191130'
print(s1[:2])

str = "abcdef"
print(str.lower())

print('*******' * 10)

ls1 = ['颜料', '计算机外部设备', '照相器材', '家用厨房电器具', '通信终端设备']
print(ls1)
rm_ls = ['计算机外部设备', '照相器材']
for item in ls1[:]:
    for rm_item in rm_ls:
        if item == rm_item:
            ls1.remove(item)
            break

print(ls1)
print('*******' * 10)

ls1 = ['B438C03D9AD1F950E053AC6DF60ADB05']
print(tuple(ls1))

print('*******' * 10)

ll1 = [1,2]
ll2 = [1,2]
ll1.sort()
ll2.sort()
print(ll1 == ll2)

print('*******' * 10)

#print(type('None'))

if 'g' in 'aaaaaaabbbbg1111':
    print('-----------')