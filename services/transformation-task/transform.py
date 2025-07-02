from pyspark.sql import SparkSession
import os
from pyspark.sql import functions as F
import logging


def category_level_kpi(order_items_df, products_df):
    logging.info("Computing category-level KPIs...")

def order_level_kpi(orders_df):
    logging.info("Computing order-level KPIs...")
    df = orders_df.withColumn("order_date", F.to_date("created_at"))

    # KPI aggregations
    kpi_df = df.groupBy("order_date").agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("num_of_item").alias("total_items_sold"),
        (F.sum(F.when(F.col("status") == "returned", 1).otherwise(0)) / F.countDistinct("order_id")).alias("return_rate"),
        F.countDistinct("user_id").alias("unique_customers")
    ).select(
        "order_date", "total_orders", "total_items_sold", "return_rate", "unique_customers"
    )
    return kpi_df



def read_csv(spark, path):
    logging.info(f"Reading CSV: {path}")
    return spark.read.option("header", "true").csv(path)


def main():
    spark = SparkSession \
        .builder \
        .appName("TransformationJob") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()
    
    s3_file_path = os.getenv("S3_FILE_PATH")
    file_type = os.getenv("FILE_TYPE")

    if not s3_file_path or not file_type:
        raise Exception("S3_FILE_PATH and FILE_TYPE must be provided.")
    s3_file_path = s3_file_path.replace("s3://", "s3a://")

    print(s3_file_path)

    df = read_csv(spark, s3_file_path)

    result = None

    if file_type == "orders":
        result = order_level_kpi(df)
    elif file_type == "order_items":
        products_path = "s3a://ecommerce-raw.amalitech-gke/products/products.csv"
        products_df = read_csv(spark, products_path)
        result = category_level_kpi(df, products_df)
    else:
        raise Exception(f"Unsupported FILE_TYPE: {file_type}")
    
    logging.info("Transformation Complete")
    (result.show())

    spark.stop()

if __name__ == "__main__":
    main()