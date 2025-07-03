from pyspark.sql import SparkSession
import os
from pyspark.sql import functions as F
import logging
import boto3
from decimal import Decimal


def category_level_kpi(order_items_df, products_df):
    logging.info("Computing category-level KPIs...")
    joined_df = order_items_df.join(
    products_df,
    order_items_df["product_id"] == products_df["id"],
    how="left"
    )
    joined_df = joined_df.withColumn("order_date", F.to_date("created_at"))
    kpi_df = joined_df.groupBy("category", "order_date").agg(
    F.round(F.sum("sale_price"), 2).alias("daily_revenue"),
    F.countDistinct("order_id").alias("num_orders"),
    F.sum(F.when(F.col("status") == "returned", 1).otherwise(0)).alias("num_returns")
    ).withColumn(
        "avg_order_value", F.round(F.col("daily_revenue") / F.col("num_orders"), 2)
    ).withColumn(
        "avg_return_rate", F.round(F.col("num_returns") / F.col("num_orders") * 100, 2)
    ).select(
        "category", "order_date", "daily_revenue", "avg_order_value", "avg_return_rate"
    )
    
    write_to_dynamo(kpi_df.collect(), "category_level")
    return kpi_df

def order_level_kpi(orders_df):
    logging.info("Computing order-level KPIs...")
    df = orders_df.withColumn("order_date", F.to_date("created_at"))

    # KPI aggregations
    kpi_df = df.groupBy("order_date").agg(
    F.countDistinct("order_id").alias("total_orders"),
    F.sum("num_of_item").alias("total_items_sold"),
    F.round(
        F.sum(F.when(F.col("status") == "returned", 1).otherwise(0)) / F.countDistinct("order_id") * 100,
        2
    ).alias("return_rate"),
    F.countDistinct("user_id").alias("unique_customers")
    ).select(
        "order_date", "total_orders", "total_items_sold", "return_rate", "unique_customers"
    )

    write_to_dynamo(kpi_df.collect(), "order_level")
    return kpi_df


def write_to_dynamo(data, kpi_type):
    """
    Dynamically write KPIs to the appropriate DynamoDB table.
    """
    dynamodb = boto3.resource("dynamodb", region_name="eu-north-1")

    if kpi_type == "order_level":
        table = dynamodb.Table("ecommerce_order_kpis")
    elif kpi_type == "category_level":
        table = dynamodb.Table("ecommerce_category_kpis")
    else:
        raise ValueError("Unsupported KPI type")
    

    for row in data:
        try:
            item = {}

            if kpi_type == "order_level":
                item = {
                    "order_date": row["order_date"].strftime("%Y-%m-%d"),
                    "total_orders": int(row["total_orders"]),
                    "total_revenue": Decimal(str(row["total_revenue"])),
                    "total_items_sold": int(row["total_items_sold"]),
                    "return_rate": Decimal(str(row["return_rate"])),
                    "unique_customers": int(row["unique_customers"])
                }

            elif kpi_type == "category_level":
                item = {
                    "category": row["category"],
                    "order_date": row["order_date"].strftime("%Y-%m-%d"),
                    "daily_revenue": Decimal(str(row["daily_revenue"])),
                    "avg_order_value": Decimal(str(row["avg_order_value"])),
                    "avg_return_rate": Decimal(str(row["avg_return_rate"]))
                }

            table.put_item(Item=item)

        except Exception as e:
            logging.error(f"Failed to insert item for {row}: {e}")

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