import pandas as pd
import os
import boto3
import logging
import json
from io import StringIO

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


schemas = {
     "orders": {
          "required_columns": {
            "user_id", "order_id", "status", "created_at", "returned_at", "shipped_at", "delivered_at", "num_of_item"
          },
          "not_null": {"user_id", "order_id"}
     },
     "products": {
        "required_columns": {"id", "category", "name", "department",
                             "brand", "retail_price", "cost"},
        "not_null": {"id", "cost"}
    },
    "orders_items": {
          "required_columns": {
            "id","user_id", "order_id", "status", 
            "created_at", "returned_at", "shipped_at", "delivered_at", "sale_price"
          },
          "not_null": {"user_id", "order_id", "id"}
     },
}

def download_from_s3(s3_uri):
    s3 = boto3.client("s3")
    bucket, key = s3_uri.replace("s3://", "").split("/", 1)
    logging.info(f"Downloading file from {s3_uri}")
    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read().decode("utf-8")

def validate_schema(df: pd.DataFrame, file_type: str):
    result = {
        "status": "passed",
        "missing_cols": [],
        "null_violations": []
    }

    if file_type not in schemas:
        raise ValueError(f"Unsupported file_type '{file_type}'")

    rule = schemas[file_type]

    # Check required columns
    missing_cols = rule["required_columns"] - set(df.columns)
    if missing_cols:
        result["status"] = "failed"
        result["missing_cols"] = list(missing_cols)


    # Check nulls
    for col in rule["not_null"]:
        if df[col].isnull().any():
            result["status"] = "failed"
            result["null_violations"].append(col)

    return result

        
def main():
    logging.info("Initaiting Validating")
    s3_input_file = os.getenv("S3_FILE_PATH", "s3://ecommerce-raw.amalitech-gke/orders/orders_part1.csv")
    file_type = os.getenv("FILE_TYPE", "orders")

    if not s3_input_file or not file_type:
        raise ValueError("S3_FILE_PATH and FILE_TYPE must be set")
    
    try:
        csv_data = download_from_s3(s3_input_file)
        reader = pd.read_csv(StringIO(csv_data), chunksize=500)
        first_chunk = next(reader)
        # reader = pd.read_csv(s3_input_file, chunksize=500)
        # first_chunk = next(reader)

        result = validate_schema(first_chunk, file_type)
        if result["status"] == "passed":
            logging.info("Validation passed.")
        else:
            logging.warning("Validation failed.")

        print(json.dumps(result)) 
            
    except Exception as e:
        logging.error(f"Validation crashed with exception: {e}")
        print(json.dumps({
            "status": "error",
            "error_message": str(e),
            "missing_cols": [],
            "null_violations": []
        }))
        raise
        


if __name__ == "__main__":
    main()