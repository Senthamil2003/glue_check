import boto3
from botocore.exceptions import ClientError
import json
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql.functions import split, col, current_timestamp
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
 


# Get job name argument
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
logger.info(f"Using database user")


s3_source_path = "s3://customer360-v1-s3bucket/raw/year=2024/"
redshift_tmp_dir = "s3://aws-glue-assets-894811220469-us-east-1/temporary/"
def get_secret():
    secret_name = "customer360_redshift_secrets"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        # Handle specific exceptions
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            print("Secrets Manager can't decrypt the protected secret text using the provided KMS key.")
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            print(f"The requested secret {secret_name} was not found.")
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            print(f"The request was invalid due to: {e.response['Error']['Message']}")
        else:
            print(f"An error occurred: {e}")
        raise e
    else:
        # Parse the secret
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            secret = base64.b64decode(get_secret_value_response['SecretBinary']).decode('utf-8')

        return json.loads(secret)

secret_data = get_secret()
print(f"Retrieved secret: {secret_data}")
# redshift_connection_options = {
#     "url": "jdbc:redshift://customer360-redshift-cluster.crwbmdpwtfxz.us-east-1.redshift.amazonaws.com:5439/customer360_db_redshift",
#     "user": "customer360",
#     "password": "Customer360",
#     "redshiftTmpDir": redshift_tmp_dir,
#     "connectionName": "Customer360-Glue-Redshift-Connection"
# }
redshift_connection_options = secret_data
# print(secret_value)



# Function to load data from S3
def load_data_from_s3():
    print("Loading data from S3...")
    return glueContext.create_dynamic_frame.from_options(
        format_options={"quoteChar": "\"", "withHeader": True, "separator": ","},
        connection_type="s3",
        format="csv",
        connection_options={
        "paths": [s3_source_path], 
        "recurse": True, 
        "jobBookmarkKeys": ["filename"],  
        "jobBookmarkKeysSortOrder": "asc"},
        transformation_ctx="Customer360_source_node"
    )

# Function to map and process locations data


# Function to write data to Redshift
def write_to_redshift(dynamic_frame, table_name):
    try:
        print(f"Writing data to Redshift ({table_name} table)...")
        glueContext.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="redshift",
            connection_options={
                **redshift_connection_options,
                "dbtable": f"public.{table_name}"
            },
            transformation_ctx=f"AmazonRedshift{table_name.capitalize()}_node"
        )
    except Exception as e:
        print(f"Error writing to Redshift ({table_name} table): {e}")
        raise

# Function to load existing data from Redshift
def load_existing_data(table_name):
    print(f"Loading existing data from Redshift ({table_name} table)...")
    return glueContext.create_dynamic_frame.from_options(
        connection_type="redshift",
        connection_options={
            **redshift_connection_options,
            "dbtable": f"public.{table_name}"
        },
        transformation_ctx=f"existing_data_{table_name}"
    )

def map_locations_schema(dynamic_frame):
    print("Mapping schema for locations and removing duplicates...")
    location_df = dynamic_frame.toDF().select("location").dropDuplicates()

    # Add current timestamp for updated_on column
    location_df = location_df.withColumn("updated_on", current_timestamp())

    return DynamicFrame.fromDF(location_df, glueContext, "mapped_location_data")
# Function to map contract details schema and add contract_id and updated_on
def map_contract_details_schema(dynamic_frame):
    print("Mapping schema for contract details...")

    # Convert DynamicFrame to DataFrame
    contract_df = dynamic_frame.toDF().select("contract").dropDuplicates()
    contract_df=contract_df.withColumn("updated_on", current_timestamp())
    
    return DynamicFrame.fromDF(contract_df, glueContext, "mapped_contract_details")

# Function to map customers schema and join contract and location data
def map_customers_schema(dynamic_frame, locations_dynamic_frame, contract_dynamic_frame):
    print("Mapping schema for customers and joining with contract and location data...")
    customer_df = dynamic_frame.toDF()
    location_df = locations_dynamic_frame.toDF()
    contract_df = contract_dynamic_frame.toDF()

    # Split 'customerid' into 'customer_id' and 'customer_name'
    customer_df = customer_df.withColumn(
        "customer_id", split(customer_df["customerid"], "-").getItem(0)
    ).withColumn(
        "customer_name", split(customer_df["customerid"], "-").getItem(1)
    )
    customer_d=customer_df.withColumn("updated_on", current_timestamp())

    # Drop 'customerid' and join with locations and contracts on respective columns
    customer_df = customer_df.drop("customerid").join(
        location_df, customer_df["location"] == location_df["location"], "left"
    ).select(
        "customer_id", "customer_name", "gender", "seniorcitizen", "partner", "dependents",
        "tenure", "age", location_df["location_id"],"updated_on"
    )

    # Convert the data types
    customer_df = customer_df.withColumn(
        "seniorcitizen", col("seniorcitizen").cast("boolean")
    ).withColumn(
        "tenure", col("tenure").cast("int")
    ).withColumn(
        "age", col("age").cast("int")
    )

    # Return the transformed DataFrame as a DynamicFrame
    return DynamicFrame.fromDF(customer_df, glueContext, "mapped_customers")
    



def filter_new_data(new_df, existing_df, key_column):
    print(f"Filtering new data for {key_column}...")
    return new_df.join(existing_df, on=key_column, how="left_anti")
    
def map_existing_customer_data(DynamicFrames):
    modified_customer_data=DynamicFrames.toDF()
    modified_customer_data=modified_customer_data.withColumn(
        "customer_id", split(modified_customer_data["customerid"], "-").getItem(0)
    )
    modified_customer_data=modified_customer_data.select("customer_id","contract")
    return DynamicFrame.fromDF(modified_customer_data, glueContext, "mapped_customers_for_contract")
    

def map_customer_contracts(customers_dynamic_frame, contract_dynamic_frame):
    """
    Map customers and contracts to create customer_contracts data.
    """
    print("Mapping schema for customer contracts...")
    
    # Convert DynamicFrames to DataFrames
    customer_df = customers_dynamic_frame.toDF()
    contract_df = contract_dynamic_frame.toDF()
    
    # Assuming there is a column `contract` in customer data which maps to `contract` in contract details
    customer_contracts_df = customer_df.join(
        contract_df, 
        customer_df["contract"] == contract_df["contract"], 
        "inner"
    ).select(
        customer_df["customer_id"], 
        contract_df["contract_id"]
    )
    customer_contracts_df=customer_contracts_df.withColumn(
        "customer_id", col("customer_id").cast("int")
    )


    return DynamicFrame.fromDF(customer_contracts_df, glueContext, "mapped_customer_contracts")

def write_customer_contracts(mapped_customer_contracts):
    """
    Write mapped customer contracts to Redshift.
    """
    try:
        print("Writing data to customer_contracts table...")
        glueContext.write_dynamic_frame.from_options(
            frame=mapped_customer_contracts,
            connection_type="redshift",
            connection_options={
                **redshift_connection_options,
                "dbtable": "public.customer_contracts"
            },
            transformation_ctx="AmazonRedshiftCustomerContracts_node"
        )
    except Exception as e:
        print(f"Error writing to Redshift (customer_contracts table): {e}")
        raise

def main():
    try:
        # Secret retrieval and initialization
        db_user = secret_data['url']
        print(f"Using database user: {db_user}")
        
        # Step 1: Load data from S3
        s3_data = load_data_from_s3()
        print("-----------//////////-------------"+str(s3_data.count())+ "------------------////--------------")
        # Step 2: Process and load contract details data
        mapped_contract_data = map_contract_details_schema(s3_data)
        existing_contract_data = load_existing_data("contract_details").toDF()
        new_contract_data = filter_new_data(mapped_contract_data.toDF(), existing_contract_data, "contract")
        write_to_redshift(DynamicFrame.fromDF(new_contract_data, glueContext, "new_contract_data"), "contract_details")
        print("Contract details table loaded successfully.")

        # Step 3: Process and load locations data
        mapped_location_data = map_locations_schema(s3_data)
        existing_location_data = load_existing_data("locations").toDF()
        new_location_data = filter_new_data(mapped_location_data.toDF(), existing_location_data, "location")
        write_to_redshift(DynamicFrame.fromDF(new_location_data, glueContext, "new_location_data"), "locations")
        print("Locations table loaded successfully.")

        # Step 4: Process and load customers data
        updated_location_data = load_existing_data("locations")
        updated_contract_data = load_existing_data("contract_details")
        mapped_customer_data = map_customers_schema(s3_data, updated_location_data, updated_contract_data)
        existing_customer_data = load_existing_data("customers").toDF()
        new_customer_data = filter_new_data(mapped_customer_data.toDF(), existing_customer_data, "customer_id")
        write_to_redshift(DynamicFrame.fromDF(new_customer_data, glueContext, "new_customer_data"), "customers")
        print("Customers table loaded successfully.")

        # Step 5: Map and load customer contracts data
        updated_customer_data = map_existing_customer_data(s3_data)
        updated_contract_data = load_existing_data("contract_details")
        mapped_customer_contracts = map_customer_contracts(updated_customer_data, updated_contract_data)
        write_customer_contracts(mapped_customer_contracts)
        print("Customer contracts table loaded successfully.")

        print("ETL job completed successfully.")

    except Exception as e:
        print(f"ETL job failed: {e}")
    finally:
        job.commit()

if __name__ == "__main__":
    main()
