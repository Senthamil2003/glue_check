import boto3
from botocore.exceptions import ClientError
import json

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

# Example usage in your Glue script
try:
    secret_data = get_secret()
    print(f"Retrieved secret: {secret_data}")
    db_user = secret_data['url']
    
    print(f"Using database user: {db_user}")
except Exception as e:
    print(f"Failed to retrieve secret: {e}")
