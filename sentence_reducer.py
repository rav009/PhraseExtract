#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import getopt


if __name__ == "__main__":
    d = {}
    blacklist = []
    threshold = 100

    try:
        opts, args = getopt.getopt(sys.argv[1:], "t:")
        for c, v in opts:
            if c == "-t":
                threshold = int(v)
    except getopt.GetoptError:
        print "Command line arguments error."
        sys.exit(-2)

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
