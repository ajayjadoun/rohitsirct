# Databricks notebook source
print("Github x Databricks")
from pyspark.sql.functions import *

# COMMAND ----------


from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import random

# Define the schema using StructType and StructField
schema = StructType([
    StructField("HRMID", StringType(), False),
    StructField("age", IntegerType(), False),
    StructField("salary", IntegerType(), False),
    StructField("position", StringType(), False),
    StructField("location", StringType(), False)
])

# Generate 20 records with random data
data = []
locations = ['Mumbai', 'Bangalore', 'Delhi', 'Chennai', 'Kolkata']
positions = ['Manager', 'Analyst', 'Director', 'Engineer', 'Intern']
for _ in range(20):  # You can replace "_" with a more descriptive variable name like "record_index"
    record = (
        'HRMID-' + str(random.randint(1, 100)),
        random.randint(20, 60),
        random.randint(40000, 100000),
        random.choice(positions),
        random.choice(locations)
    )
    data.append(record)

# Create a DataFrame with the specified schema
df = spark.createDataFrame(data, schema=schema)

# Show the DataFrame
df.display()


# COMMAND ----------

df.filter(col("age")>35).display()