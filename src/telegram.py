import json
import requests
from os import getenv

base_url = f'https://api.telegram.org/bot{getenv("EIABOT_TGKEY")}/'
channel_id = getenv('EIABOT_TGCHAT')

def post_message(message: str):
    data = {
        'chat_id' : channel_id,
        'text' : message,
        'parse_mode' : 'HTML'
    }
    r = requests.post(url=base_url + 'sendMessage', data=data)
    return r.json()