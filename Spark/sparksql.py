#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

if __name__ == "__main__":
    sparksession = SparkSession.builder\
        .appName("query phrase count")\
        .master("yarn-cluster")\
        .enableHiveSupport()\
        .getOrCreate()
    df = sparksession.sql("SELECT * FROM case_phrase limit 100")
    print df.collect()