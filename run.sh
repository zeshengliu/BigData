#!/usr/bin/env bash
cur_date="`date +%Y-%m-%d --date "1 days ago"`"
del_date="`date +%Y-%m-%d --date "7 days ago"`"
echo $cur_date
echo $del_date
cd /home/ads_bgn/liuzesheng/newpro
spark-submit \
--num-executors 400 \
--driver-memory 35G \
--executor-memory 35G \
--executor-cores 4 \
--conf spark.submit.deployMode=cluster \
--conf spark.speculation=true \
--conf spark.sql.broadcastTimeout=1000000 \
--conf spark.executor.memoryOverhead=8192 \
--conf spark.default.parallelism=12000 \
--conf spark.sql.shuffle.partitions=12000 \
--conf spark.shuffle.service.enabled=true \
--conf spark.driver.maxResultSize=10g \
--conf spark.network.timeout=1000000 \
--class UserInterestProfile \
target/scala-2.11/simple-project_2.11-1.0.jar --date $cur_date \