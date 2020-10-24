from lxml import etree

html = """
<div>
    <ul>
        <li><p>这是第1个li</p></li>
        <li><p>这是第2个li</p></li>
        <li><p>这是第3个li</p></li>
    </ul>
    这是div的内容
</div>
"""
html_obj = etree.HTML(html)
p_content = html_obj.xpath("//div/ul/li[1]/p/text()")
print("获取到的内容：", p_content)

