#!/bin/bash

#For running this script, in terminal: bash /home/cloudera/Desktop/commands.sh

clear
echo Project by Pedro, Nam, Thahn Le, Munkhdalai!

echo Creating directories and cleaning data in HDFS...
hadoop fs -mkdir -p /user/cloudera/hive_tables
hadoop fs -mkdir -p /user/cloudera/hive_tables/all_info
hadoop fs -rm -r /user/cloudera/hive_tables/all_info/*
#hadoop fs -rm -r /user/cloudera/hive_tables/temp_all_info
#hadoop fs -rm -r /user/cloudera/temp_info/*

echo Create table in Hive...
hive -f /home/cloudera/Desktop/createtable.hql;

echo Safe to run jars.
