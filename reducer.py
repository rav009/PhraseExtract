# -*- coding: utf-8 -*-
import sys

d = {}
c = {}

showcasenumber = 0

for line in sys.stdin:
    w = line.split("\t")
    d.setdefault(w[0], 0)
    d[w[0]] = d[w[0]] + 1
    c.setdefault(w[0], [])
    c[w[0]].append(w[1])

for k in d.keys():
    if d[k] > 200:
        if showcasenumber == 1:
            print k.replace(';', ' ').replace('\n', '') + "\t" + str(d[k]) + "\t" + ";".join(c[k]).replace('\n', '')
        else:
            print k.replace(';', ' ').replace('\n', '') + "\t" + str(d[k])
