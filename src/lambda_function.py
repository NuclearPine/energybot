# Copyright 2022 NuclearPine

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import requests
import boto3
import telegram as tg
from datetime import datetime
from time import time
from os import getenv
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr

eiakey = getenv("EIAKEY")
# Connect to DynamoDB for logging successful posts
ddb = boto3.resource('dynamodb')
table = ddb.Table(getenv('EIABOT_TABLE'))

# Helper functions for adding thousands commas and symbols
def add_com(value):
    return "{:,}".format(value)

def format_num(value, symbol):
    string = "{:,}".format(value)
    if value > 0:
        string = '+' + string
    if symbol != None:
        string = string[0] + symbol + string[1:]
    return string

            

# Lambda handler for weekly stocks data
def stocks_handler(event, context):

    # Request and clean data from the EIA
    # Series ID reference
    # Weekly crude oil stocks incl SPR: PET.WCRSTUS1.W
    # Weekly crude oil stocks excl SPR: PET.WCESTUS1.W
    # Weekly SPR stocks: PET.WCSSTUS1.W
    # Weekly total gasoline stocks: PET.WGTSTUS1.W
    # Weekly total distillate stocks PET.WDISTUS1.W
    
    header = {
    "frequency": "weekly",
    "data": [
        "value"
    ],
    "facets": {
        "series": [
            "WCESTUS1",
            "WCRSTUS1",
            "WCSSTUS1",
            "WGTSTUS1",
            "WDISTUS1"
        ]
    },
    "start": None,
    "end": None,
    "sort": [
        {
            "column": "period",
            "direction": "desc"
        }
    ],
    "offset": 0,
    "length": 10}

    url = f'https://api.eia.gov/v2/petroleum/stoc/wstk/data/?api_key={eiakey}'
    r = requests.get(url=url, headers={'X-Params' : json.dumps(header)})
    data = json.loads(r.content)['response']['data']
    
    series_end = data[0]['period']
    total_stocks = [i['value'] for i in data if i['series'] == 'WCRSTUS1']
    com_stocks = [i['value'] for i in data if i['series'] == 'WCESTUS1']
    spr_stocks = [i['value'] for i in data if i['series'] == 'WCSSTUS1']
    gas_stocks = [i['value'] for i in data if i['series'] == 'WGTSTUS1']
    dist_stocks = [i['value'] for i in data if i['series'] == 'WDISTUS1']
    
    # Check DDB table if a post was already made for this data
    ddb_response = table.query(
        KeyConditionExpression=Key('dataset').eq('crude_stocks'),
        FilterExpression=Attr('series_end').eq(series_end)
    )

    if len(ddb_response['Items']) == 0:
        
        # Format the message to be posted
        end_date = datetime.strptime(series_end, '%Y-%m-%d')
        message = f'<b>Weekly petroleum product stocks as of {end_date.strftime("%B %d, %Y")}</b>\n'
        message += f'All numbers in thousands of barrels, weekly change in parenthesis\n\n'
        message += 'Crude Oil\n'
        message += 'Commercial:   ' + add_com(com_stocks[0]) + f' ({format_num(com_stocks[0]-com_stocks[1], None)})\n'
        message += 'SPR:    ' + add_com(spr_stocks[0]) + f' ({format_num(spr_stocks[0]-spr_stocks[1], None)})\n'
        message += 'Total:    ' + add_com(total_stocks[0]) + f' ({format_num(total_stocks[0]-total_stocks[1], None)})\n\n'
        message += 'Gasoline:   ' + add_com(gas_stocks[0]) + f' ({format_num(gas_stocks[0]-gas_stocks[1], None)})\n'
        message += 'Distillates:    ' + add_com(dist_stocks[0]) + f' ({format_num(dist_stocks[0]-dist_stocks[1], None)})\n\n'
        message += 'Source: US Energy Information Administration\n#petroleum #stocks'
        
        # Post the message and check for a good response
        tg_response = tg.post_message(message)
        if tg_response['ok'] == True:
            table.put_item(
                Item={
                    'dataset' : 'crude_stocks',
                    'timestamp' : int(time()),
                    'series_end' : series_end
                })
            return {'statusCode': 200, 'body': json.dumps({'post_made' : True, 'new_data' : True, 'tg_response' : tg_response})}

        else:
            return {'statusCode': 502, 'body': json.dumps({'post_made' : False, 'new_data' : True, 'tg_response' : tg_response})}

    else:
        return {'statusCode' : 200, 'body' : json.dumps({'post_made' : False, 'new_data' : False, 'tg_response' : None})}

def futures_handler(event, context): #WIP

    header = {
    "frequency": "daily",
    "data": [
        "value"
    ],
    "facets": {
        "series": [
            "RCLC1", # Cushing WTI
            "EER_EPMRR_PE1_Y35NY_DPG", # NY Harbor RBOB gasoline
            "EER_EPD2F_PE1_Y35NY_DPG" # NY Harbor No. 2 diesel
        ]
    },
    "start": None,
    "end": None,
    "sort": [
        {
            "column": "period",
            "direction": "desc"
        }
    ],
    "offset": 0,
    "length": 6}

    url = f'https://api.eia.gov/v2/petroleum/pri/fut/data/?api_key={eiakey}'
    r = requests.get(url=url, headers={"X-Params":json.dumps(header)})
    data = json.loads(r.content)['response']['data']

    ngheader = {
    "frequency": "daily",
    "data": [
        "value"
    ],
    "facets": {
        "series": [
            "RNGC1" # Henry Hub nat gas
        ]
    },
    "start": None,
    "end": None,
    "sort": [
        {
            "column": "period",
            "direction": "desc"
        }
    ],
    "offset": 0,
    "length": 2}
    
    url = f'https://api.eia.gov/v2/natural-gas/pri/fut/data/?api_key={eiakey}'
    r = requests.get(url=url, headers={"X-Params":json.dumps(ngheader)})
    ngdata = json.loads(r.content)['response']['data']
    
    series_end = data[0]['period']
    wti = [i['value'] for i in data if i['series'] == 'RCLC1']
    gasoline = [i['value'] for i in data if i['series'] == 'EER_EPMRR_PE1_Y35NY_DPG']
    no2 = [i['value'] for i in data if i['series'] == 'EER_EPD2F_PE1_Y35NY_DPG']
    ng = [i['value'] for i in ngdata if i['series'] == 'RNGC1']

    ddb_response = table.query(
        KeyConditionExpression=Key('dataset').eq('futures'),
        FilterExpression=Attr('series_end').eq(series_end)
    )
    
    if len(ddb_response['Items']) == 0:
        
        wti_chg = format_num(round(wti[0]-wti[1], 2), '$')
        wti_pct = format_num(round((wti[0]-wti[1]) / wti[1] * 100, 2), '%')
        gasoline_chg = format_num(round(gasoline[0]-gasoline[1], 3), '$')
        gasoline_pct = format_num(round((gasoline[0]-gasoline[1]) / gasoline[1] * 100, 2), '%')
        no2_chg = format_num(round(no2[0]-no2[1], 3), '$')
        no2_pct = format_num(round((no2[0]-no2[1]) / no2[1] * 100, 2), '%')
        ng_chg = format_num(round(ng[0]-ng[1], 3), '$')
        ng_pct = format_num(round((ng[0]-ng[1]) / ng[1] * 100, 2), '%')

        end_date = datetime.strptime(series_end, '%Y-%m-%d')
        message = f'<b>NYMEX closing prompt-month futures prices for {end_date.strftime("%B %d, %Y")}</b>\n'
        message += 'Change since last recorded close in parenthesis\n\n'
        message += f'<b>Crude Oil:</b> ${wti[0]}/bbl ({wti_chg}, {wti_pct})\n'
        message += f'<b>RBOB Gasoline:</b> ${gasoline[0]}/gal ({gasoline_chg}, {gasoline_pct})\n'
        message += f'<b>Heating Oil:</b> ${no2[0]}/gal ({no2_chg}, {no2_pct})\n'
        message += f'<b>Natural Gas:</b> ${ng[0]}/MMBTU ({ng_chg}, {ng_pct})\n\n'
        message += 'Source: US Energy Information Administration\n#petroleum #prices'

        tg_response = tg.post_message(message)
        if tg_response['ok'] == True:

            table.put_item(
                Item={
                    'dataset' : 'futures',
                    'timestamp' : int(time()),
                    'series_end' : series_end
                })

            return {'statusCode': 200, 'body': json.dumps({'post_made' : True, 'new_data' : True, 'tg_response' : tg_response})}

        else:
            return {'statusCode': 502, 'body': json.dumps({'post_made' : False, 'new_data' : True, 'tg_response' : tg_response})}

    else:
        return {'statusCode' : 200, 'body' : json.dumps({'post_made' : False, 'new_data' : False, 'tg_response' : None})}

