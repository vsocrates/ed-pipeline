from pyspark.sql import SparkSession
import os


# Set PYSPARK_PYTHON
os.environ["PYSPARK_PYTHON"] = "./temp_envs/py3env/bin/python"

spark = SparkSession\
    .builder\
    .getOrCreate()

try:
    from cchlib import schematic
except ImportError:
    raise ImportError('Error importing cchlib module. Run build.py first')

# Import and instantiate your code from modules here

spark.stop()
