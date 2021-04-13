#!/usr/bin/env python
# -*- coding: utf-8 -*-

count = 0
while True:
    str = input("Enter quit: ")
    # check for valid passwd
    if str == "quit":
        break
    count = count + 1
    if count % 3 > 0:
        continue
    print("Please input quit!")

print('Quit loop successfully!')
