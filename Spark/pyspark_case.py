#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pyspark
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName("case_analytics")
    sc = SparkContext(conf=conf)
    # rdd = sc.textFile('adl://intellimax.azuredatalakestore.net/input.txt')
    wordsRDD = sc.textFile("adl://intellimax.azuredatalakestore.net/input.txt",10)\
		     .flatMap(lambda x: [i for i in x.split("|")[2:] if i.strip() is not ''])\
		     .flatMap(lambda x: [i for i in x.split("\\n") if i.strip() is not ''])\
		     .persist()
    stop_sentences = wordsRDD.map(lambda x: (x,1))\
		     	     .reduceByKey(lambda x, y: x+y)\
		             .filter(lambda x: x[1] > 100)

    stop_sentences = sc.broadcast(sentencesRDD.collect())
