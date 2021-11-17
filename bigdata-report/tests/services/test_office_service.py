# -*- coding: utf-8 -*-
from report.commons.connect_kudu import prod_execute_sql
from report.services.office_expenses_service import get_office_bill_jiebaword

if __name__ == "__main__":
    final_list = get_office_bill_jiebaword()

    print(len(final_list))













