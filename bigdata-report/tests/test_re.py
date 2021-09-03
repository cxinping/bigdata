# -*- coding: utf-8 -*-
import re


def test():
    #匹配规则必须含有u,可以没有r
    #这里第一个分组的问号是懒惰匹配,必须这么做
    PATTERN = r'([\u4e00-\u9fa5]{2,5}?(?:省|自治区|市)){0,1}([\u4e00-\u9fa5]{2,7}?(?:区|县|州)){0,1}([\u4e00-\u9fa5]{2,7}?(?:镇)){0,1}([\u4e00-\u9fa5]{2,7}?(?:村|街|街道)){0,1}([\d]{1,3}?(号)){0,1}'

    data_list = ['北京市', '陕西省西安市雁塔区', '西班牙', '北京市海淀区', '黑龙江省佳木斯市汤原县', '内蒙古自治区赤峰市',
    '贵州省黔南州贵定县', '新疆维吾尔自治区伊犁州奎屯市']

    for data in data_list:
        #data_utf8 = data.decode('utf8')
        data_utf8 = data
        #print(data_utf8)
        country = data
        province = ''
        city = ''
        district = ''
        #pattern = re.compile(PATTERN3)
        pattern = re.compile(PATTERN)
        m = pattern.search(data_utf8)
        print(m)
        if not m:
            print(country + '|||')
        continue

        print(m.group())
        country = '中国'
        if m.lastindex >= 1:
            province = m.group(1)
        if m.lastindex >= 2:
            city = m.group(2)
        if m.lastindex >= 3:
            district = m.group(3)
        out = 'result=> %s|%s|%s|%s' %(country, province, city, district)
        print(out)

def match_address(data):
    import re
    #PATTERN1 = r'([\u4e00-\u9fa5]{2,5}?(?:省|自治区|市)){0,1}([\u4e00-\u9fa5]{2,7}?(?:区|县|州)){0,1}([\u4e00-\u9fa5]{2,7}?(?:镇)){0,1}([\u4e00-\u9fa5]{2,7}?(?:村|街|街道)){0,1}([\d]{1,3}?(号)){0,1}'
    PATTERN1 = r'([\u4e00-\u9fa5]{2,5}?(?:省|自治区|市)){0,1}([\u4e00-\u9fa5]{2,7}?(?:区|县|州)){0,1}([\u4e00-\u9fa5]{2,7}?(?:镇)){0,1}([\u4e00-\u9fa5]{2,7}?(?:村|街|街道)){0,1}([\d]{1,3}?(号)){0,1}'

    # \u4e00-\u9fa5 匹配任何中文
    # {2,5} 匹配2到5次
    # ? 前面可不匹配
    # (?:pattern) 如industr(?:y|ies) 就是一个比 'industry|industries' 更简略的表达式。意思就是说括号里面的内容是一个整体是以y或者ies结尾的单词
    pattern = re.compile(PATTERN1)
    province = ''
    city = ''
    p3 = ''
    p4 = ''
    p5 = ''
    p6 = ''
    m = pattern.search(data)
    #print(m.lastindex)
    if not m:
        print('None')
    if m.lastindex >= 1:
        province = m.group(1)
    if m.lastindex >= 2:
        city = m.group(2)
    if m.lastindex >= 3:
        p3 = m.group(3)
    if m.lastindex >= 4:
        p4 = m.group(4)
    if m.lastindex >= 5:
        p5 = m.group(5)
    if m.lastindex >= 6:
        p6 = m.group(6)
    out = '%s|%s|%s|%s|%s|%s' % (province, city, p3, p4, p5, p6)
    return province, city

def match_address2(place , key):

    ssxq = ['省', '市', '县', '区']
    indexes0 = [place.find(x) for x in ssxq]
    indexes = [x for x in indexes0 if x > 0]
    indexes2 = sorted(indexes)
    indexes2.insert(0, -1)
    address = []

    for i, x in enumerate(indexes2):
        if i == len(indexes2) - 1:
            continue
        else:
            address.append(place[x + 1:indexes2[i + 1] + 1])

    for addr in address:

        if addr.find(key) > -1:
            return addr
    return None

if __name__ == '__main__':
    #data = str(input("请输入文本:"))
    #data = "安徽省淮南县大通区大通街道某某某"
    data = "贵州省黔南州贵定县"
    #data = '贵州省黔南州贵定县'
    province, city = match_address(data)
    print(province, city)

    idx = city.find('市')+1
    xian_city = city[idx:]
    print(xian_city)
    print('--------------------------------')
    # print(''.join(re.compile(r"[\u4E00-\u9FA5\w]+市").findall(data)[0]))
    # print(''.join(re.compile(r"[\u4E00-\u9FA5\w]+省").findall(data)[0]))
    # print(''.join(re.compile(r"[\u4E00-\u9FA5\w]+县").findall(data)[0]))

    place = '上海市金山区金一东路80号37909988'
    address = match_address2(place=place,key='市')
    print(address)

