# -*- coding: UTF-8 -*-

rows = range(1, 10)
for row in rows:
    for column in range(1, row + 1):
        print("%d * %d = %d \t" % (column, row, column * row), end="")

    print("")

	
	