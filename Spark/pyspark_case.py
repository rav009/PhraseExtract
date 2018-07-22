#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pyspark
from pyspark import SparkContext, SparkConf


def gensentence(line):
    l = line.split("|")
    for c in l[2:4]:
        s = c.split("\\n")
        return [(i, 1) for i in s if i != '']


if __name__ == "__main__":
    conf = SparkConf().setAppName("case_analytics")
    sc = SparkContext(conf=conf)
    # rdd = sc.textFile('adl://intellimax.azuredatalakestore.net/input.txt')
    stop_sentencesRDD = sc.textFile("adl://intellimax.azuredatalakestore.net/input.txt",10)\
		     .flatMap(lambda x: [i for i in x.split("|")[2:] if i.strip() is not ''])\
		     .flatMap(lambda x: [i for i in x.split("\\n") if i.strip() is not ''])\
		     .map(lambda x: (x,1))\
		     .reduceByKey(lambda x, y: x+y)\
		     .filter(lambda x: x[1] > 100)

    stop_sentences = sc.broadcast(sentencesRDD.collect())
