# -*- coding: utf-8 -*-
"""
https://zhuanlan.zhihu.com/p/382810968
"""

class Pagination(object):
    def __init__(self, current_page, all_count, per_page_num=2, pager_count=11):
        """
        封装分页相关数据
        :param current_page: 当前页
        :param all_count:    数据库中的数据总条数
        :param per_page_num: 每页显示的数据条数
        :param pager_count:  最多显示的页码个数
        """
        try:
            current_page = int(current_page)
        except Exception as e:
            current_page = 1

        if current_page < 1:
            current_page = 1

        self.current_page = current_page

        self.all_count = all_count
        self.per_page_num = per_page_num

        # 总页码
        all_pager, tmp = divmod(all_count, per_page_num)
        if tmp:
            all_pager += 1
        self.all_pager = all_pager

        self.pager_count = pager_count
        self.pager_count_half = int((pager_count - 1) / 2)

    @property
    def get_all_pager(self):
        return self.all_pager

    @property
    def start(self):
        return (self.current_page - 1) * self.per_page_num

    @property
    def end(self):
        return self.current_page * self.per_page_num




book_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26]

current_page = 1
all_count  = len(book_list)
page_obj = Pagination(current_page=current_page,all_count=all_count,per_page_num=10)
page_queryset = book_list[page_obj.start:page_obj.end]
print(page_queryset)
print(page_obj.start,page_obj.end)
print(page_obj.get_all_pager)

