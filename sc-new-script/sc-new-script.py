import sys
from awsglue.context import GlueContext
from pyspark.context import SparkContext

sc = SparkContext()
glueContext = GlueContext(sc)

# Get parameters passed by Lambda
args = sys.argv
bucket = args[args.index('--source_bucket') + 1]
key = args[args.index('--source_key') + 1]

print(f"Processing file s3://{bucket}/{key}")

# Load the file and perform transformations
datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="csv",  # Change format if needed
    connection_options={
        "paths": [f"s3://{bucket}/{key}"],
        "recurse": True
    }
)
print("hello sentha from my branch")

# Perform ETL and write back to S3 or another target
datasource.show()
