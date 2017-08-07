import json
import boto3
import boto3.dynamodb.types

# Load the service resources in the desired region.
# Note: AWS credentials should be passed as environment variables
# or through IAM roles.
dynamodb = boto3.resource('dynamodb', region_name="us-east-1")
kinesis = boto3.client('kinesis', region_name="us-east-1")

# Load the DynamoDB table.
ddb_table_name = "articles"
ks_stream_name = "articles"
table = dynamodb.Table(ddb_table_name)

# Get the primary keys.
ddb_keys_name = [a['AttributeName'] for a in table.attribute_definitions]

# Scan operations are limited to 1 MB at a time.
# Iterate until all records have been scanned.
response = None
while True:
    if not response:
        # Scan from the start.
        response = table.scan()
    else:
        # Scan from where you stopped previously.
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])

    for i in response["Items"]:
        # Get a dict of primary key(s).
        ddb_keys = {k: i[k] for k in i if k in ddb_keys_name}
        # Serialize Python Dictionaries into DynamoDB notation.
        ddb_data = boto3.dynamodb.types.TypeSerializer().serialize(i)["M"]
        ddb_keys = boto3.dynamodb.types.TypeSerializer().serialize(ddb_keys)["M"]
        # The record must contain "Keys" and "NewImage" attributes to be similar
        # to a DynamoDB Streams record. Additionally, you inject the name of
        # the source DynamoDB table in the record so you can use it as an index
        # for Amazon ES.
        record = {"Keys": ddb_keys, "NewImage": ddb_data, "SourceTable": ddb_table_name}
        # Convert the record to JSON.
        record = json.dumps(record)
        # Push the record to Amazon Kinesis.
        res = kinesis.put_record(
            StreamName=ks_stream_name,
            Data=record,
            PartitionKey=str(i["wordpressId"]))
        print(res)

    # Stop the loop if no additional records are
    # available.
    if 'LastEvaluatedKey' not in response:
        break