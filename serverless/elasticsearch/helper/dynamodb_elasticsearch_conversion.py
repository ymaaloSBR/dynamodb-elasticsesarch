import re
import logging
import json

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
    # Convert the record that is DynamoDB formatted into a plain JSON. e.g. Remove keys that indicate DynamoDB type
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
