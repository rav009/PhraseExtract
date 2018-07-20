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
    sentencesRDD = sc.textFile("file:///home/rav009/PycharmProjects/pywei/PhraseExtract/Spark/testtext", 10)\
                  .flatMap(lambda x: gensentence(x)).repartition(10)\
                  .reduceByKey(lambda x, y: x+y)\
                  .filter(lambda x: x > 100)\
                  .persist(pyspark.StorageLevel.useDisk)

    sentences = sc.broadcast(sentencesRDD.collect())
