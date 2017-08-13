from __future__ import print_function

import base64
import decimal
import json
import logging
import os
import re
import time

import boto3
from elasticsearch import Elasticsearch

from lib.index import get_index_settings  # ignore import error, this deploys correctly.

# Process DynamoDB Stream records and insert the object in ElasticSearch
# Use the Table name as index and doc_type name
# Force index refresh upon all actions for close to realtime reindexing
# Use IAM Role for authentication
# Properly unmarshal DynamoDB JSON types. Binary NOT tested.

FORMAT = '%(levelname)s - %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


def decode_kinesis_data(record):
    logging.info("Decoding Kinesis Record...")
    dec = base64.b64decode(record['kinesis']['data']).decode("utf-8")
    logging.info("Kinesis Record Data Decoded:")
    logging.info(dec)
    return dec


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

    try:
        response = table.update_item(
            Key={
                'wordpressId': record['id']
            },
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values,
            ExpressionAttributeNames={
                '#id': '_id'
            },
            ReturnValues="UPDATED_NEW"
        )
    except Exception as e:
        logging.exception("Error in updating DynamoDB record")
        return

    logging.info("Dynamo record updated: " + json.dumps(response, indent=2, cls=DecimalEncoder))


def process_stream(event, context):
    # Connect to ES
    es = Elasticsearch(
        [os.environ['ES_ENDPOINT']],
    )

    logging.info("Cluster info:")
    logging.info(es.info())

    # Loop over the DynamoDB Stream records
    for record in event['Records']:

        logging.info("New Record to process:")
        logging.info(json.dumps(record))
        try:

            if record['eventName'] == "INSERT" or record['eventName'] == "aws:kinesis:record":
                insert_document(es, record)
            elif record['eventName'] == "REMOVE":
                remove_document(es, record)
            elif record['eventName'] == "MODIFY":
                modify_document(es, record)

        except Exception as e:
            logging.exception(e)
            continue


# Process MODIFY events
def modify_document(es, record):
    table = get_table(record)
    logging.info("Dynamo Table: " + table)

    doc_id = generate_id(record)
    logging.info("KEY")
    logging.info(doc_id)

    # Unmarshal the DynamoDB JSON to a normal JSON
    doc = json.dumps(unmarshal_json(record['dynamodb']['NewImage']))

    logging.info("Updated document:")
    logging.info(doc)

    # We reindex the whole document as ES accepts partial docs
    es.index(index=table,
             body=doc,
             id=doc_id,
             doc_type=table,
             refresh=True)

    logging.info("Success - Updated index ID: " + doc_id)


# Process REMOVE events
def remove_document(es, record):
    table = get_table(record)
    logging.info("Dynamo Table: " + table)

    doc_id = generate_id(record)
    logging.info("Deleting document ID: " + doc_id)

    es.delete(index=table,
              id=doc_id,
              doc_type=table,
              refresh=True)

    logging.info("Successly removed")


# Process INSERT events
def insert_document(es, record):
    table = get_table(record)
    logging.info("Dynamo Table: " + table)

    # Create index if missing
    if not es.indices.exists(table):
        logging.info("Create missing index: " + table)

        es.indices.create(table,
                          body=get_index_settings())

        logging.info("Index created: " + table)

    # Unmarshal the DynamoDB JSON to a normal JSON
    doc = ''
    if record['eventName'] == "aws:kinesis:record":
        record = json.loads(decode_kinesis_data(record))
        doc = json.dumps(unmarshal_json(record['NewImage']))
    else:
        doc = json.dumps(unmarshal_json(record['dynamodb']['NewImage']))

    logging.info("New document to Index:")
    logging.info(doc)

    new_id = generate_id(record)
    logging.info("Indexing into Elasticsearch...")
    try:
        es.index(index=table,
                 body=doc,
                 id=new_id,
                 doc_type=table,
                 refresh=True)
    except Exception as e:
        logging.exception("Dynamo Record with id " + new_id + " has an error")
        update_dynamodb({'doc': doc, 'id': int(new_id), 'table': table})
        return

    logging.info("Success - New Index ID: " + new_id)


# Return the dynamoDB table that received the event. Lower case it
def get_table(record):
    p = re.compile('arn:aws:(?:dynamodb|kinesis):.*?:(?:table|stream)/([\w-]+)(?:.)*')
    m = p.match(record['eventSourceARN'])
    if m is None:
        raise Exception("Table not found in SourceARN")
    return m.group(1).lower()


# Generate the ID for ES. Used for deleting or updating item later
def generate_id(record):
    logging.info("Generating ID")
    if 'dynamodb' in record:
        keys = unmarshal_json(record['dynamodb']['Keys'])
    else:
        keys = unmarshal_json(record['Keys'])
    logging.info("Keys in record: " + json.dumps(keys))
    # Concat HASH and RANGE key with | in between
    new_id = ""
    i = 0
    for key, value in keys.items():
        if i > 0:
            new_id += "|"
        new_id += str(value)
        i += 1

    return new_id


# Unmarshal a JSON that is DynamoDB formatted
def unmarshal_json(node):
    logging.info("Unmarshalling record...")
    data = {"M": node}
    return unmarshal_value(data, True)


# ForceNum will force float or Integer to
def unmarshal_value(node, force_num=False):
    for key, value in node.items():
        if key == "NULL":
            return None
        if key == "S" or key == "BOOL":
            return value
        if key == "N":
            if force_num:
                return int_or_float(value)
            return value
        if key == "M":
            data = {}
            for key1, value1 in value.items():
                data[key1] = unmarshal_value(value1, True)
            return data
        if key == "BS" or key == "L":
            data = []
            for item in value:
                data.append(unmarshal_value(item))
            return data
        if key == "SS":
            data = []
            for item in value:
                data.append(item)
            return data
        if key == "NS":
            data = []
            for item in value:
                if force_num:
                    data.append(int_or_float(item))
                else:
                    data.append(item)
            return data


# Detect number type and return the correct one
def int_or_float(s):
    try:
        return int(s)
    except ValueError:
        return float(s)
