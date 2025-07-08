import pandas as pd
import os
import boto3
import logging
import json
from io import StringIO
from pathlib import Path

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
            "id", "user_id", "order_id", "status",
            "created_at", "returned_at", "shipped_at", "delivered_at", "sale_price"
        },
        "not_null": {"user_id", "order_id", "id"}
    },
}

s3 = boto3.client("s3")

RAW_BUCKET = "ecommerce-raw.amalitech-gke"
DELTA_BUCKET = "ecommerce-delta.amalitech-gke"


def list_files(bucket, prefix):
    """Return a list of files for a given prefix in an S3 bucket."""
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" in response:
        return [obj["Key"] for obj in response["Contents"] if obj["Key"].endswith(".csv")]
    return []


def check_product_files():
    product_prefix = "products"
    product_files = list_files(RAW_BUCKET, product_prefix)

    if product_files:
        logging.info("Found product files in raw bucket")
        return {
            "validated": True,
            "type": "files",
            "file":product_files
        }

    # Check if delta table exists
    delta_files = list_files(DELTA_BUCKET, "delta/products/_delta_log")
    if delta_files:
        logging.info("Product Delta table already  exists")
        return {
            "validated": True,
            "type": "table",
            "file":product_files
        }
    
    logging.error("No product files or delta table found. Validation failed.")
    return {
            "validated": False,
            "type": "files",
            "file":[]
        }


def check_order_files():
    order_prefix = "orders"
    order_files = list_files(RAW_BUCKET, order_prefix)

    if order_files:
        logging.info("Found order files")
        return {
            "validated": True,
            "type": "files",
            "file":order_files
        }
    else:
        logging.warning("No order files found")
        return {
            "validated": False,
            "type": "files",
            "file":[]
        }


def check_order_items_files():
    order_items_prefix = "order_items"
    order_items_files = list_files(RAW_BUCKET, order_items_prefix)

    if order_items_files:
        logging.info("Found order items files")
        return {
            "validated": True,
            "type": "files",
            "file":order_items_files
        }
    else:
        logging.error("No order items files found. Validation failed.")
        return {
            "validated": False,
            "type": "files",
            "file":order_items_files
        }

def download_from_s3(s3_uri):
    s3 = boto3.client("s3")
    bucket, key = s3_uri.replace("s3://", "").split("/", 1)
    logging.info(f"Downloading file from {s3_uri}")
    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read().decode("utf-8")


def write_result_to_s3(result: dict, file_type: str, file_name):
    s3 = boto3.client("s3")
    bucket = "ecommerce-validitor-checks"
    key = f"validation-results/{file_type}/{file_name}.json"
    logging.info(f"Writing result to s3://{bucket}/{key}")
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(result))

def quarantine_file(file_name, file_type):
     # Copy the object
    s3 = boto3.client("s3")
    s3.copy_object(
        Bucket="ecommerce-validitor-checks",
        CopySource={'Bucket': RAW_BUCKET, 'Key': f"{file_type}/{file_name}"},
        Key=f"quarantine/{file_type}/{file_name}"
    )

    # Delete the original object
    s3.delete_object(Bucket=RAW_BUCKET, Key=f"{file_type}/{file_name}")

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

def validate_files_and_summarize(files: list, file_type: str):
    total_files = len(files)
    passed = 0
    failed_files = []

    for file in files:
        file_name = Path(file).name.replace(".csv", "")
        try:
            csv_data = download_from_s3(f"s3://{RAW_BUCKET}/{file}")
            reader = pd.read_csv(StringIO(csv_data), chunksize=500)
            first_chunk = next(reader)

            result = validate_schema(first_chunk, file_type)

            if result["status"] == "passed":
                passed += 1
            else:
                failed_files.append(file)
                quarantine_file(file, file_type)

        except Exception as e:
            logging.error(f"Validation failed for {file}: {e}")
            failed_files.append(file)

    summary = {
        "file_type": file_type,
        "total_files": total_files,
        "number_validated": passed,
        "files_failed_to_validate": failed_files,
        "validation_success": passed > 0 
    }

    return summary





def main():
    logging.info("Initiating validation...")
        # Get the timestamp from ENV or use current UTC time as fallback
    event_time_str = os.getenv("EVENT_TIME")

    try: 
        if event_time_str:

            # Normalize the timestamp to a safe filename format
       
            final_summary = []

            product_response = check_product_files()
            order_response = check_order_files()
            order_items_response = check_order_items_files()

            # Product Validation check
            if product_response["validated"] and product_response["type"] == "files":
                product_summary = validate_files_and_summarize(product_response["file"], "products")
                final_summary.append(product_summary)
            elif product_response["validated"] and product_response["type"] == "table":
                final_summary.append({
                "file_type": "table",
                "total_files": 0,
                "number_validated": 0,
                "files_failed_to_validate": [],
                "validation_success": True
            })
            else:
                final_summary.append({
                "file_type": "table",
                "total_files": 0,
                "number_validated": 0,
                "files_failed_to_validate": [],
                "validation_success": False
            })
                

            # Order Validation check
            if order_response["validated"]:
                order_summary = validate_files_and_summarize(order_response["file"], "orders")
                final_summary.append(order_summary)
            else:
                final_summary.append({
                "file_type": "files",
                "total_files": 0,
                "number_validated": 0,
                "files_failed_to_validate": [],
                "validation_success": False
            })


            # Oder Items Validation Check
            if order_items_response["validated"]:
                order_items_summary = validate_files_and_summarize(order_items_response["file"], "orders_items")
                final_summary.append(order_items_summary)

            else:
                final_summary.append({
                "file_type": "files",
                "total_files": 0,
                "number_validated": 0,
                "files_failed_to_validate": [],
                "validation_success": False
            })

            # Write combined final summary to S3

            validated_status = all(item["validation_success"] for item in final_summary)

            final_result = {
                "event_id": event_time_str,
                "validated_status": validated_status,
                "summary": final_summary
            }
            
            print(json.dumps(final_result, indent=2))
            write_result_to_s3(final_result, file_type="all", file_name=f"validation_summary_{event_time_str}")

        else:
            logging.error("EVENT_TIME environment variable not provided. Cannot proceed.")
            return
    except Exception as e:
        logging.error(f"Fatal error during validation: {e}", exc_info=True)

if __name__ == "__main__":
    main()
