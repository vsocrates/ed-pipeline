#%%

from pyspark.sql import SparkSession

from pyspark.sql.functions import lower, col, explode, split, length, substring, trim, upper, regexp_replace, concat, lit, when, desc, min, first, unix_timestamp, datediff, round as spark_round, min as spark_min, max as spark_max

import os

#%%
# Set PYSPARK_PYTHON

#%%

# Set PYSPARK_PYTHON
os.environ["PYSPARK_PYTHON"] = "./temp_envs/py3env/bin/python"

spark = SparkSession\
    .builder\
    .enableHiveSupport()\
    .getOrCreate()

#%%
base_data_loc = "/projects/emergency_department/research/raw/omop_cch/"
domain = spark.read.parquet(base_data_loc + 'domain') 

#%%
domain.toPandas()
# %%
