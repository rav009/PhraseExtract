#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark import SparkContext, SparkConf
import uuid
import re


def genuuid():
    return str(uuid.uuid4()).replace('-', '')


def hideemail(s):
    edict = {}
    emailregex = r"[-_\w\.]{0,64}@[-\w]{1,63}\.*[-\w]{1,63}"
    es = re.findall(emailregex, s)
    if len(es) > 0:
        for e in es:
            if e not in edict.values():
                u = genuuid()
                s = s.replace(e, u)
                edict[u] = e
    return s, edict


def removeStopSentences(c):
    for caseid, casetext in c:
        n = casetext
        for s in stop_sentences.value:
            n = n.replace(s[0].strip(), '')
        n = n.replace("\\n", " ")
        yield caseid, n


if __name__ == "__main__":
    conf = SparkConf().setAppName("case_analytics")
    sc = SparkContext(conf=conf)
    # rdd = sc.textFile('adl://intellimax.azuredatalakestore.net/input.txt')
    RDD = sc.textFile("file:///home/rav009/PycharmProjects/PhraseExtract/Spark/testtext", 10)\
        .map(lambda x: [i for i in x.split("|") if i.strip() is not u''])\
        .map(lambda x: (x[0], '\\n'.join(x[1:])))\
        .persist()
    stop_sentencesRDD = RDD.flatMap(lambda x: [i for i in x[1].split("\\n") if i.strip() is not u''])\
        .map(lambda x: (x,1))\
        .reduceByKey(lambda x, y: x+y)\
        .filter(lambda x: x[1] > 100)

    stop_sentences = sc.broadcast(stop_sentencesRDD.collect())
    print stop_sentences.value

    RDD2 = RDD.mapPartitions(lambda x: removeStopSentences(x))
    print RDD2.collect()



