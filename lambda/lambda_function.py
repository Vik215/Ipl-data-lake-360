import json
import boto3

# Create S3 client
s3 = boto3.client("s3")


def lambda_handler(event, context):

    try:
        print("========== S3 EVENT RECEIVED ==========")
        print(json.dumps(event, indent=4))

        # Extract bucket and object information
        bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
        object_key = event["Records"][0]["s3"]["object"]["key"]

        print(f"Bucket Name : {bucket_name}")
        print(f"Object Key  : {object_key}")

        # -----------------------------
        # Validation 1 : Check CSV file
        # -----------------------------
        if not object_key.lower().endswith(".csv"):
            return {
                "statusCode": 400,
                "body": json.dumps("Invalid file. Only CSV files are allowed.")
            }

        print("CSV validation passed.")

        # -----------------------------
        # Read file from S3
        # -----------------------------
        response = s3.get_object(
            Bucket=bucket_name,
            Key=object_key
        )

        content = response["Body"].read().decode("utf-8")

        # Split file into lines
        lines = content.splitlines()

        if len(lines) == 0:
            return {
                "statusCode": 400,
                "body": json.dumps("CSV file is empty.")
            }

        # -----------------------------
        # Read header
        # -----------------------------
        header = lines[0]

        print("CSV Header:")
        print(header)

        columns = header.split(",")

        print("Columns Found:")
        print(columns)

        # -----------------------------
        # Required Columns
        # -----------------------------
        required_columns = [
            "id",
            "season"
        ]

        missing_columns = []

        for column in required_columns:

            if column not in columns:
                missing_columns.append(column)

        if len(missing_columns) > 0:

            print("Missing Columns:")
            print(missing_columns)

            return {
                "statusCode": 400,
                "body": json.dumps(
                    f"Missing required columns: {missing_columns}"
                )
            }

        print("All required columns exist.")

        # -----------------------------
        # Row Count
        # -----------------------------
        total_rows = len(lines) - 1

        print(f"Total Rows : {total_rows}")

        print("Validation Successful.")

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "Validation Successful",
                    "bucket": bucket_name,
                    "file": object_key,
                    "rows": total_rows,
                    "columns": len(columns)
                }
            )
        }

    except Exception as e:

        print("ERROR")
        print(str(e))

        return {
            "statusCode": 500,
            "body": json.dumps(str(e))
        }