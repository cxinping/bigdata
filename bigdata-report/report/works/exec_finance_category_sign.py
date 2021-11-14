# -*- coding: utf-8 -*-
from report.services.office_expenses_service import query_checkpoint_42_commoditynames, get_office_bill_jiebaword, \
    pagination_office_records
from report.commons.logging import get_logger
from report.services.common_services import (insert_finance_shell_daily, update_finance_shell_daily,
                                             operate_finance_category_sign, clean_finance_category_sign,
                                             query_finance_category_signs,
                                             query_finance_category_sign, pagination_finance_shell_daily_records)

log = get_logger(__name__)


def import_data():
    # part1
    unusual_id = '26'
    category_classify = '01'
    type_str = '会议费'
    available_category_name = query_checkpoint_42_commoditynames()
    operate_finance_category_sign(unusual_id=unusual_id, category_names=available_category_name,
                                  category_classify=category_classify, sign_status='0')

    unusual_id = '26'
    category_classify = '02'
    type_str = '会议费'
    available_category_name = query_checkpoint_42_commoditynames()
    operate_finance_category_sign(unusual_id=unusual_id, category_names=available_category_name,
                                  category_classify=category_classify, sign_status='0')

    # part2
    unusual_id = '42'
    category_classify = '01'
    type_str = '办公费'
    available_category_name = query_checkpoint_42_commoditynames()
    operate_finance_category_sign(unusual_id=unusual_id, category_names=available_category_name,
                                  category_classify=category_classify, sign_status='0')

    unusual_id = '42'
    category_classify = '02'
    type_str = '办公费'
    available_category_name = query_checkpoint_42_commoditynames()
    operate_finance_category_sign(unusual_id=unusual_id, category_names=available_category_name,
                                  category_classify=category_classify, sign_status='0')

    # part2
    unusual_id = '55'
    category_classify = '01'
    type_str = '车辆使用费'
    available_category_name = query_checkpoint_42_commoditynames()
    operate_finance_category_sign(unusual_id=unusual_id, category_names=available_category_name,
                                  category_classify=category_classify, sign_status='0')

    unusual_id = '55'
    category_classify = '02'
    type_str = '车辆使用费'
    available_category_name = query_checkpoint_42_commoditynames()
    operate_finance_category_sign(unusual_id=unusual_id, category_names=available_category_name,
                                  category_classify=category_classify, sign_status='0')


import_data()
