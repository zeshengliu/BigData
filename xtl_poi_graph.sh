#!/usr/bin/env bash
spark-submit \
--num-executors 300 \
--driver-memory 35G \
--executor-memory 35G \
--executor-cores 4 \
--conf spark.submit.deployMode=cluster \
--conf spark.speculation=true \
--conf spark.executor.memoryOverhead=6000 \
--conf spark.default.parallelism=12000 \
--conf spark.sql.shuffle.partitions=12000 \
--conf spark.shuffle.service.enabled=true \
--conf spark.driver.maxResultSize=10g \
--conf spark.network.timeout=8000 \
--class PoiGraph \
simple-project_2.11-1.0.jar