#!/usr/bin/env python
# -*- coding: utf-8 -*-
# copy the log4j.properties.template to log4j.properties and modify it, then move it to the path you execute the cmd
# spark-submit --master yarn --deploy-mode client \
# --driver-java-options "-Dlog4j.configuration=log4j.properties" \
# --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j.properties" \


from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName("log-test")
    sc = SparkContext(conf=conf)
    # useless
    sc.setLogLevel("WARN")

    c = sc.textFile("adl://intellimax.azuredatalakestore.net/test.txt", 10) \
        .count()
    log4jLogger = sc._jvm.org.apache.log4j
    log = log4jLogger.LogManager.getLogger(__name__)
    # this warning will only show in the driver logs
    log.warn("Hello World!") 