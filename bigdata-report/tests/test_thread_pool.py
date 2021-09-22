# -*- coding: utf-8 -*-
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from datetime import datetime

def spider(page):
    time.sleep(page)
    print(f"crawl task{page} finished")
    return page

def main():
    with ThreadPoolExecutor(max_workers=5) as t:
        obj_list = []
        x = datetime.now()
        for page in range(1, 5):
            obj = t.submit(spider, page)
            obj_list.append(obj)

        # for future in as_completed(obj_list):
        #     data = future.result()
        #     print(f"main: {data}")

        print('共耗时' + str(datetime.now() - x))

if __name__ == "__main__":
    main()

# 执行结果
# crawl task1 finished
# main: 1
# crawl task2 finished
# main: 2
# crawl task3 finished
# main: 3
# crawl task4 finished
# main: 4