# Databricks notebook source
print("hello worldDD")
# divmoddd


# COMMAND ----------

a=[i for i in range(1,11)]
d=0
for i in a:
    d+=i
print(d)

# COMMAND ----------

print("jai shree ram")

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import Row

# Create a Spark session
spark = SparkSession.builder.appName("CreateDataFrame").getOrCreate()

# Sample data
data = [
    ("John", 80000, "Manager", "New York"),
    ("Alice", 60000, "Engineer", "San Francisco"),
    ("Bob", 55000, "Analyst", "Chicago"),
    ("Emma", 75000, "Designer", "Los Angeles"),
    ("David", 70000, "Developer", "Seattle"),
    ("Olivia", 65000, "Associate", "Boston"),
    ("Michael", 90000, "Director", "Houston"),
    ("Sophia", 85000, "Manager", "Miami"),
    ("William", 58000, "Coordinator", "Atlanta"),
    ("Ava", 63000, "Analyst", "Denver")
]

# Define the schema for the DataFrame
columns = ["name", "salary", "position", "location"]

# Create a DataFrame directly from the data and schema
df = spark.createDataFrame(data, columns)

# Show the DataFrame
df.display()
