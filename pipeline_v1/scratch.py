# %%

import os

from pyspark.sql import SparkSession

# %%
# Set PYSPARK_PYTHON

# %%

# Set PYSPARK_PYTHON
os.environ["PYSPARK_PYTHON"] = "./temp_envs/py3env/bin/python"

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

# %%
base_data_loc = "/projects/emergency_department/research/raw/omop_cch/"
domain = spark.read.parquet(base_data_loc + "domain")

# %%
domain.toPandas()
# %%
