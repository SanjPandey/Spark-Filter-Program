# Filter in Spark


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create a SparkSession
spark = SparkSession.builder \
    .appName("SparkExample") \
    .getOrCreate()

# Define a schema for the DataFrame
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

# Sample data
data = [("John", 30), ("Alice", 25), ("Bob", 35)]

# Create a DataFrame
df = spark.createDataFrame(data, schema)

# Show the DataFrame
df.show()

# Perform some transformations and actions
# Example: Filter people with age greater than 30
filtered_df = df.filter(df["age"] > 30)
filtered_df.show()

# Stop the SparkSession
spark.stop()