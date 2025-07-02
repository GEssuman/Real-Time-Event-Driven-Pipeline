from pyspark.sql import SparkSession


spark = SparkSession \
    .builder \
    .appName("") \
    .getOrCreate()


data = [
        ("Alice", 25),
        ("Bob", 30),
        ("Charlie", 35)
    ]
    
# Define the DataFrame schema.
columns = ["Name", "Age"]

# Create a DataFrame from the data.
df = spark.createDataFrame(data, schema=columns)

# Show the DataFrame content.
print("=== Test DataFrame ===")
df.show()
