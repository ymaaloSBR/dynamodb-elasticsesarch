import boto3
import logging
import json
import re


FORMAT = '%(levelname)s - %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb', region_name="us-east-1")

table = dynamodb.Table('articles')

response = None
update_expression = 'set paragraphs=:p'


def get_paragraphs(article):
    paragraphs = re.split(r"(?:\s{4}|\n\n|\n\s\n)", article['contentText'])
    paragraphs = list(filter(None, paragraphs))
    if '\n' in paragraphs:
        paragraphs.remove('\n')
    result = []
    tickers = []
    if 'tickers' in article:
        tickers = article['tickers']
    for p in paragraphs:
        matches = [ticker for ticker in tickers if ticker in p]
        paragraph = {'tickers': matches, 'contentText': p}
        result.append(paragraph)
    logging.info("Creating parapgrahs: " + " ".join(str(x) for x in result))
    return result

limit = -1
count = 0
while True:
    if not response:
        # Scan from the start.
        response = table.scan()
    else:
        # Scan from where you stopped previously.
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])

    for item in response["Items"]:
        logging.info("Updating paragraphs attribute for item with ID: " + str(item['wordpressId']))
        if 'contentText' in item:
            expression_attribute_values = {':p': get_paragraphs(item)}
            update_response = table.update_item(Key={'wordpressId': item['wordpressId']},
                                                UpdateExpression=update_expression,
                                                ExpressionAttributeValues=expression_attribute_values)
            count += 1
        if count == limit:
            quit()

    if 'LastEvaluatedKey' not in response:
        break
