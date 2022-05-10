import json
import requests
import boto3
import telegram as tg
from datetime import datetime
from time import time
from os import getenv
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr

def lambda_handler(event, context):
    
    ddb = boto3.resource('dynamodb')
    table = ddb.Table('eiabot-log')
    # Series ID reference
    # Weekly crude oil stocks incl SPR: PET.WCRSTUS1.W
    # Weekly crude oil stocks excl SPR: PET.WCESTUS1.W
    # Weekly SPR stocks: PET.WCSSTUS1.W
    # Weekly finished gasonline stocks:
    # Weekly finished distillate stocks 
    
    eiakey = getenv("EIAKEY")
    params = {
        'api_key' : eiakey,
        'out' : 'json',
        'series_id' : 'PET.WCRSTUS1.W;PET.WCESTUS1.W;PET.WCSSTUS1.W',
        'num' : '52'
    }
    
    r = requests.get(url=f'https://api.eia.gov/series/', params=params)
    data = json.loads(r.content)['series']
    
    total_stocks = [i[1] for i in data[0]['data']]
    com_stocks = [i[1] for i in data[1]['data']]
    spr_stocks = [i[1] for i in data[2]['data']]
    series_end = int(data[0]['end'])

    ddb_response = table.query(
        KeyConditionExpression=Key('dataset').eq('crude_stocks'),
        FilterExpression=Attr('series_end').eq(series_end) & Attr('post_success').eq(True)
    )

    if len(ddb_response['Items']) == 0:

        end_date = datetime.strptime(data[0]['end'], '%Y%m%d')
        message = f'<b>Crude oil stocks for week ending {end_date.strftime("%B %d, %Y")} (weekly change)</b>\n\n'
        message += f'Commercial:    {com_stocks[0]/1000}M     ({(com_stocks[0]-com_stocks[1])/1000}M)\n'
        message += f'SPR:                   {spr_stocks[0]/1000}M    ({(spr_stocks[0]-spr_stocks[1])/1000}M)\n'
        message += f'Total:                 {total_stocks[0]/1000}M    ({(total_stocks[0]-total_stocks[1])/1000}M)\n'
        
        tg_response = tg.post_message(message)
        if tg_response['ok'] == True:
            table.put_item(
                Item={
                    'dataset' : 'crude_stocks',
                    'timestamp' : int(time()),
                    'query_success' : True,
                    'series_end' : series_end,
                    'post_success' : True
                })
            return {'statusCode': 200, 'body': json.dumps({'post_made' : True, 'tg_response' : tg_response})}

        else:
            return {'statusCode': 502, 'body': json.dumps({'post_made' : False, 'tg_reponse' : tg_response})}

    else:
        return {'statusCode' : 200, 'body' : json.dumps({'post_made' : False})}