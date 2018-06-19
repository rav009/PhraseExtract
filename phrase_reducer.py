#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import getopt


if __name__ == "__main__":

    d = {}
    c = {}

    threshold = 2000
    showcasenumber = False

    try:
        opts, args = getopt.getopt(sys.argv[1:], "t:c")
        for c, v in opts:
            if c == "t":
                threshold = int(v)
            if c == "c":
                showcasenumber = True
    except getopt.GetoptError:
        print "Command line arguments error."
        sys.exit(-2)

    for line in sys.stdin:
        w = line.split("\t")
        d.setdefault(w[0], 0)
        d[w[0]] = d[w[0]] + 1
        c.setdefault(w[0], [])
        c[w[0]].append(w[1])

    for k in d.keys():
        if d[k] > threshold:
            if showcasenumber == 1:
                print k.replace(';', ' ').replace('\n', '') + "\t" + str(d[k]) + "\t" + ";".join(c[k]).replace('\n', '')
            else:
                print k.replace(';', ' ').replace('\n', '') + "\t" + str(d[k])
