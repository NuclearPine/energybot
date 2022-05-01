import json, os, requests
from time import time

import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr

ddb = boto3.resource("dynamodb")
table = ddb.Table('eiabot-log')
# Series ID reference
# Weekly crude oil stocks incl SPR: PET.WCRSTUS1.W
# Weekly crude oil stocks excl SPR: PET.WCESTUS1.W
# Weekly SPR stocks: PET.WCSSTUS1.W
# Weekly finished gasonline stocks:
# Weekly finished distillate stocks 

eiakey = json.load(open('keys.json', 'r'))['eiakey']
params = {
    'api_key' : eiakey,
    'out' : 'json',
    'series_id' : 'PET.WCRSTUS1.W;PET.WCESTUS1.W;PET.WCSSTUS1.W',
    'num' : '52'
}

r = requests.get(url=f'https://api.eia.gov/series/', params=params)
data = json.loads(r.content)['series']

if len(data) == 3:
    total_stocks = data[0]
    com_stocks = data[1]
    spr_stocks = data[2]
    series_end = int(total_stocks['end'])

    response = table.query(
        KeyConditionExpression=Key('dataset').eq('crude_stocks'),
        FilterExpression=Attr('series_end').eq(series_end) & Attr('post_success').eq(True)
    )

    if len(response['Items']) == 0:
        print("updated EIA data found - making new post")

        table.put_item(
            Item={
                'dataset' : 'crude_stocks',
                'timestamp' : int(time()),
                'query_success' : True,
                'series_end' : series_end,
                'post_success' : True
            }
        )

    else:
        print("no new EIA data available")

