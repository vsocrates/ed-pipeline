#!/bin/bash
PROJECT_NAME=$(git config --local remote.origin.url|sed -n 's#.*/\([^.]*\)\.git#\1#p')

hdfs dfs -mkdir /projects/cch/$PROJECT_NAME

hdfs dfs -mkdir /projects/cch/$PROJECT_NAME/clinical
hdfs dfs -mkdir /projects/cch/$PROJECT_NAME/clinical/raw
hdfs dfs -mkdir /projects/cch/$PROJECT_NAME/clinical/interim
hdfs dfs -mkdir /projects/cch/$PROJECT_NAME/clinical/processed
hdfs dfs -mkdir /projects/cch/$PROJECT_NAME/clinical/raw/omop

hdfs dfs -mkdir /projects/cch/$PROJECT_NAME/research
hdfs dfs -mkdir /projects/cch/$PROJECT_NAME/research/raw
hdfs dfs -mkdir /projects/cch/$PROJECT_NAME/research/interim
hdfs dfs -mkdir /projects/cch/$PROJECT_NAME/research/processed
hdfs dfs -mkdir /projects/cch/$PROJECT_NAME/research/raw/omop