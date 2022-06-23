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
import pandas as pd
import matplotlib.pyplot as plt
import requests
import telegram as tg
from datetime import datetime
from os import getenv
import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr

eiakey = getenv("EIAKEY")
# Connect to DynamoDB for logging successful posts
ddb = boto3.resource('dynamodb', region_name='us-east-1', aws_access_key_id=getenv('AWS_KEY_ID'), aws_secret_access_key=getenv('AWS_SECRET'))
table = ddb.Table(getenv('EIABOT_TABLE'))

# Lambda handler for weekly stocks data
def stocks_handler(event, context):

    # Load/transform the WPSR stocks table into a pandas dataframe
    df = pd.read_csv('https://ir.eia.gov/wpsr/table4.csv') 
    last_date = datetime.strptime(df.columns[1], '%m/%d/%y').date()
    wk_date = datetime.strptime(df.columns[2], '%m/%d/%y').date()
    yr_date = datetime.strptime(df.columns[4], '%m/%d/%y').date()
    
    # Check DDB table if a post was already made for this data
    ddb_response = table.query(
        KeyConditionExpression=Key('dataset').eq('crude_stocks') & Key('last_date').eq(last_date.isoformat())
    )

    if len(ddb_response['Items']) == 0:
        
        # additional dataframe formatting
        products = df.iloc[[0,1,9,10,16],[1,2,3,4,5]]
        products.index = ['total','com','spr','gas','dist']
        products.columns = pd.Index(['last', '1wk', 'diff', '1y', 'pct'])
        def f1(x):
            if x > 0:
                return f'+{str(x)}'
            else:
                return str(x)
        def f2(x):
            if x > 0:
                return f'+{str(x)}%'
            else:
                return f'{str(x)}%'

        products['diff'] = products['diff'].apply(f1)
        products['pct'] = products['pct'].apply(f2)

        # Create table image
        def create_img(img_path: str):
            fig_background_color = '#ffffff'
            plt.figure(linewidth=2, tight_layout={'pad':1}, figsize=(6,3), facecolor=fig_background_color)
            plt.suptitle(f'U.S. Petroleum Product Stocks, Week Ending {wk_date.isoformat()}\n Million Barrels')

            col_labels = [last_date.isoformat(), wk_date.isoformat(), 'Difference', 
                        yr_date.isoformat(), '1Y Change']
            row_labels = ['Crude oil', '    Commercial', '    SPR', 'Gasoline', 'Distillates']
            row_colors = ['#ffffff', '#ccfccc', '#ffffff', '#ccfccc', '#ffffff',]
            col_colors = ['#cce4fc', '#cce4fc', '#cce4fc', '#cce4fc', '#cce4fc',]
            cell_colors = []
            for i in range(5):
                if i % 2 == 0:
                    cell_colors.append(['#ffffff', '#ffffff', '#ffffff', '#ffffff', '#ffffff'])
                else:
                    cell_colors.append(['#ccfccc', '#ccfccc', '#ccfccc', '#ccfccc', '#ccfccc'])

            cell_text = []
            for i in products.index:
                cell_text.append(list(products.loc[i]))
                    
            ax = plt.gca()
            ax.get_xaxis().set_visible(False)
            ax.get_yaxis().set_visible(False)
            plt.box(on=None)

            table = plt.table(cellText=cell_text, colLabels=col_labels, rowLabels=row_labels, 
                            rowColours=row_colors, cellColours=cell_colors, colColours=col_colors,
                            loc='center')

            footer_text = 'Source: EIA Weekly Petroleum Status Report'
            footer_text += '\ngithub.com/NuclearPine/energybot'
            plt.figtext(.05, .05, footer_text, horizontalalignment='left')

            table.scale(1,1.5)

            fig = plt.gcf()
            plt.savefig(img_path, 
                        dpi=150, 
                        edgecolor=fig.get_edgecolor(), 
                        facecolor=fig.get_facecolor())
        
        create_img('/tmp/eiabot-table.png')
        message = f'Weekly petroleum product stocks for week ending {last_date.isoformat()}'
        
        # Post the message and check for a good response
        tg_response = tg.post_image(message=message, image='/tmp/eiabot-table.png')
        if tg_response['ok'] == True:
            table.put_item(
                Item={
                    'dataset' : 'crude_stocks',
                    'last_date' : last_date.isoformat()
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

