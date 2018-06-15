#!/usr/bin/env python
# -*- coding: utf-8 -*-


import sys

for line in sys.stdin:
    l = line.split("|")
    for c in l[2:4]:
        s = c.split("\\n")
        ss = [i for i in s if i != '']
        for i in ss:
            print i