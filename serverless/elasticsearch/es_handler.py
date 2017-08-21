from __future__ import print_function

import base64
import json
import logging
import os
from elasticsearch import Elasticsearch, ElasticsearchException

# ignore import errors, this deploys correctly.
from helper.update_dynamodb import update_dynamodb
from helper.index import get_index_settings
from helper.dynamodb_elasticsearch_conversion import get_table, generate_id, unmarshal_json, get_paragraphs

# Process DynamoDB/Kinesis Stream records and insert the object in ElasticSearch
# Use the Table name as index and doc_type name
# Force index refresh upon all actions for close to realtime reindexing
# Properly unmarshal DynamoDB JSON types. Binary NOT tested.

FORMAT = '%(levelname)s - %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


# Entry point, handles the input event from Dynamo/Kinesis
def process_stream(event, context):
    # Connect to ES
    es = Elasticsearch(
        [os.environ['ES_ENDPOINT']],
    )

    logging.info("Cluster info:")
    logging.info(es.info())

    # Loop over the DynamoDB/Kinesis Stream records
    for record in event['Records']:

        logging.info("New Record to process:")
        logging.info(json.dumps(record))
        try:
            # Determine the event type being processed
            if record['eventName'] == "INSERT" or record['eventName'] == "aws:kinesis:record":
                insert_document(es, record)
            elif record['eventName'] == "REMOVE":
                remove_document(es, record)
            elif record['eventName'] == "MODIFY":
                modify_document(es, record)

        except Exception as e:
            logging.exception(e)
            continue


def decode_kinesis_data(record):
    logging.info("Decoding Kinesis Record...")
    dec = base64.b64decode(record['kinesis']['data']).decode("utf-8")
    logging.info("Kinesis Record Data Decoded:")
    logging.info(dec)
    return dec


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

    logging.info("Successfully removed")


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
    if record['eventName'] == "aws:kinesis:record":
        record = json.loads(decode_kinesis_data(record))
        doc = json.dumps(unmarshal_json(record['NewImage']))
    else:
        doc = json.dumps(unmarshal_json(record['dynamodb']['NewImage']))

    logging.info("Document unmarshalled: " + doc)
    # Add paragraphs attribute
    doc = get_paragraphs(doc)

    logging.info("New document to Index:")
    logging.info(doc)

    # Generate the id that will be used to stored in Elasticsearch
    new_id = generate_id(record)
    logging.info("Indexing into Elasticsearch...")
    try:
        es.index(index=table,
                 body=doc,
                 id=new_id,
                 doc_type=table,
                 refresh=True)
    except ElasticsearchException:
        logging.exception("Dynamo Record with id " + new_id + " has an error")
        update_dynamodb({'doc': doc, 'id': int(new_id), 'table': table})
        return

    logging.info("Success - New Index ID: " + new_id)
