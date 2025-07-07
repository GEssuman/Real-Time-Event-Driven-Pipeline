from pyspark.sql import SparkSession
import os
from pyspark.sql import functions as F
import logging
import boto3
from decimal import Decimal
from pyspark.sql.utils import AnalysisException

# Define Delta output paths
BASE_DELTA_PATH = "s3a://ecommerce-delta.amalitech-gke/delta"
DELTA_PRODUCTS_PATH = f"{BASE_DELTA_PATH}/products"

BASE_RAW_PATH="s3a://ecommerce-raw.amalitech-gke"
RAW_PRODUCTS_PATH = f"{BASE_RAW_PATH}/products"
RAW_ORDERS_PATH = f"{BASE_RAW_PATH}/orders"
RAW_ORDER_ITEMS_PATH = f"{BASE_RAW_PATH}/order_items"

# def list_files(bucket, prefix):


#     """Return a list of files for a given prefix in an S3 bucket."""
#     response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
#     if "Contents" in response:
#         return [obj["Key"] for obj in response["Contents"] if obj["Key"].endswith(".csv")]
#     return []

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
    
    # write_to_dynamo(kpi_df.collect(), "category_level")
    return kpi_df

def order_level_kpi(orders_df, order_items_df):
    logging.info("Computing order-level KPIs...")

    df = order_items_df.select(
        "order_id", 
        "sale_price"
    ).join(
        orders_df,
        on="order_id",
        how="left"
    )

    df = df.withColumn("order_date", F.to_date("created_at"))

    kpi_df = df.groupBy("order_date").agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("num_of_item").alias("total_items_sold"),
        F.round(
            F.sum(F.when(F.col("status") == "returned", 1).otherwise(0)) / F.countDistinct("order_id") * 100, 2
        ).alias("return_rate"),
        F.countDistinct("user_id").alias("unique_customers"),
        F.round(F.sum("sale_price"), 2).alias("total_revenue")
    ).select(
        "order_date", "total_orders", "total_items_sold", "return_rate",
        "unique_customers", "total_revenue"
    )
    unprocessed_orders_df = orders_df.join(order_items_df, on="order_id", how="left_anti")
    # write_to_dynamo(kpi_df.collect(), "order_level")
    return kpi_df, unprocessed_orders_df


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
    return spark.read.option("header", "true").option("inferSchema", "true").csv(path)


def main():
    spark = SparkSession \
        .builder \
        .appName("TransformationJob") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .getOrCreate()
        # .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    

    try:
        product_df = spark.read.format("parquet").load(DELTA_PRODUCTS_PATH)
        exists = True
    except AnalysisException:
        exists = False
        product_df = None
    if not exists:
        # No parquet yet — load from raw
        product_df = read_csv(spark, RAW_PRODUCTS_PATH)
        product_df.write.format("parquet").mode("overwrite").save(DELTA_PRODUCTS_PATH)

    # # Check for new files and upsert

    # available_files = list_files("ecommerce-raw.amalitech-gke", "products")
    # if available_files:
    #     new_product_df = read_csv(spark, RAW_PRODUCTS_PATH)

    #     # Write to staging in append or overwrite mode
    #     unchanged_product_df = product_df.join(new_product_df, on="product_id", how="left_anti")

    #     product_df = unchanged_product_df.union(new_product_df)
        
    #     product_df.write.mode("append").parquet(DELTA_PRODUCTS_PATH)


    products_df = read_csv(spark, RAW_PRODUCTS_PATH)
    orders_df = read_csv(spark, RAW_ORDERS_PATH)

    orders_items_df = read_csv(spark, RAW_ORDER_ITEMS_PATH)


    category_level_df = category_level_kpi(order_items_df=orders_items_df, products_df=products_df)
    order_level_df, unprocessed_orders_df = order_level_kpi(orders_df, orders_items_df)



    category_level_df.show()
    order_level_df.show()
    unprocessed_orders_df.show()
    spark.stop()




if __name__ == "__main__":
    main()