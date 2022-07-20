#%%

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, datediff, desc, explode, first, length, lit, lower
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import split, substring, trim, unix_timestamp, upper, when

#%%
# Set PYSPARK_PYTHON

#%%

# Set PYSPARK_PYTHON
os.environ["PYSPARK_PYTHON"] = "./temp_envs/py3env/bin/python"

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

#%%
base_data_loc = "/projects/emergency_department/research/raw/omop_cch/"
domain = spark.read.parquet(base_data_loc + "domain")

#%%
domain.toPandas()
# %%
