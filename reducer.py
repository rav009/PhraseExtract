# -*- coding: utf-8 -*-
import sys

d = {}

for line in sys.stdin:
    d.setdefault(line, 0)
    d[line] = d[line] +1

for k in d.keys():
    if d[k] > 40:
        print k.replace(';', ' ').replace('\n', '')+"\t" + str(d[k])
