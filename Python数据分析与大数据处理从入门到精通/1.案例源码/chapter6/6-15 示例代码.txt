# -*- coding: UTF-8 -*-

import time, threading
global_score=100
def update_score(score):
    for i in range(200000):
        global global_score
        global_score = global_score + score
        global_score = global_score - score

threads = []
for i in range(10):
    thread = threading.Thread(target=update_score, args=(i*10,))
    threads.append(thread)

for i in threads:
    i.start()

for i in threads:
    i.join()

print("global_score值：",global_score)


