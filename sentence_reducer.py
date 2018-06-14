# -*- coding: utf-8 -*-
import sys

d = {}
blacklist = []
threshold = 100

for line in sys.stdin:
    if line in blacklist:
        continue
    d.setdefault(line, 0)
    d[line] = d[line] + 1
    if d[line] > threshold:
        blacklist.append(line)
        d.pop(line)

for l in blacklist:
    if l.strip() != "":
        print l,