import time
import boto3
import logging
import os
import json
import decimal


def get_epoch_from_dict(input_dict):
    try:
        time_string = next(iter(input_dict.values()))
    except AttributeError as e:
        return input_dict
    pattern = '%Y-%m-%dT%H:%M:%S.000Z'
    try:
        epoch = int(time.mktime(time.strptime(time_string, pattern)))
    except ValueError as e:
        logging.error("Format error in the date.")
        time_string = time_string[1:]
        epoch = int(time.mktime(time.strptime(time_string, pattern)))
    return epoch


def update_dynamodb(record):
    logging.info("Updating DynamoDB record with id " + str(record['id']) + ".")
    doc_json = json.loads(record['doc'])
    logging.info("Successfully converted the document from a string to a dictionary.")
    dynamodb = boto3.resource('dynamodb',
                              region_name=os.environ['REGION'],
                              aws_access_key_id=os.environ['AWS_ACCESS'],
                              aws_secret_access_key=os.environ['AWS_SECRET'])
    logging.info("Successfully connected to DynamoDB service.")
    table = dynamodb.Table(record['table'])
    logging.info("Successfully retrieved DynamoDB table: " + record['table'])

    logging.info("Converting the createdAt value: " + json.dumps(doc_json['createdAt']))
    created_at_value = get_epoch_from_dict(doc_json['createdAt'])
    logging.info("Converting the modifedAt value: " + json.dumps(doc_json['modifiedAt']))
    modified_at_value = get_epoch_from_dict(doc_json['modifiedAt'])
    expression_attribute_values = {':c': created_at_value,
                                   ':m': modified_at_value, }
    update_expression = "remove #id set createdAt=:c, modifiedAt=:m"
    if '_id' in doc_json:
        oid_value = next(iter(doc_json['_id'].values()))
        logging.info("Updating 'oid' value from map to : " + str(oid_value))
        expression_attribute_values[':o'] = oid_value
        update_expression += ", oid=:o"

    if not doc_json['defaultPublication']:
        logging.error("Updating defaultPublication from boolean to empty Map")
        expression_attribute_values[':p'] = {}
        update_expression += ", defaultPublication=:p"

    logging.info("Updating 'createdAt' value from map to  : " + str(created_at_value))
    logging.info("Updating 'modifiedAt' value from map to : " + str(modified_at_value))

    key = {'wordpressId': record['id']}
    expression_attribute_names = {'#id': '_id'}
    try:
        response = table.update_item(
            Key=key,
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values,
            ExpressionAttributeNames=expression_attribute_names,
            ReturnValues="UPDATED_NEW"
        )
    except Exception as e:
        logging.exception("Error in updating DynamoDB record")
        return

    logging.info("Dynamo record updated: " + json.dumps(response, indent=2, cls=DecimalEncoder))


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)
