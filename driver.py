from pyspark.sql import SparkSession
import os
from distutils import util


# Set PYSPARK_PYTHON
os.environ["PYSPARK_PYTHON"] = "./temp_envs/py3env/bin/python"

# Zip Modules
os.system("zip modules.zip -r modules/*.py")

spark = SparkSession\
    .builder\
    .getOrCreate()

spark.sparkContext.addPyFile("/home/cdsw/modules.zip")


from cchlib import copymerge


spark.stop()

# Remove Module Zip
os.system("rm modules.zip")
