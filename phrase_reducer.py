#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import getopt


def log(err):
    with open("/tmp/phrase_mapper.log", "a+") as f:
        f.write(str(err) + "\n")
        

if __name__ == "__main__":
    threshold = 2000
    showcasenumber = False
    last_phrase = ""
    c = 0
    casenumbers = []

    try:
        opts, args = getopt.getopt(sys.argv[1:], "t:c")
        for c, v in opts:
            if c == "-t":
                threshold = int(v)
            if c == "-c":
                showcasenumber = True
    except getopt.GetoptError:
        log("Wrong arguments!")
        sys.exit(-2)

    for line in sys.stdin:
        w = line.split("\t")
        if not w[1].isdigit():
            log("Case number is not digits.")
            sys.exit(-2)
        if last_phrase == w[0]:
            c += 1
            if showcasenumber:
                casenumbers.add(w[1])
        else:
            if c > threshold:
                if showcasenumber:
                    print last_phrase .replace(';', ' ').replace('\n', '') + "\t" + str(c) + "\t" + ";".join(casenumbers).replace('\n', '')
                else:
                    print last_phrase.replace(';', ' ').replace('\n', '') + "\t" + str(c) + "\t"
            last_phrase = w[0]
            casenumbers = []
            c = 1
