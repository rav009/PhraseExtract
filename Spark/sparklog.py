#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName("log-test")
    sc = SparkContext(conf=conf)

    c = sc.textFile("adl://intellimax.azuredatalakestore.net/test.txt", 10) \
        .count()
    log4jLogger = sc._jvm.org.apache.log4j
    log = log4jLogger.LogManager.getLogger(__name__)
    log.warn("Hello World!")