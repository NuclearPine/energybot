import json
import requests
from os import getenv

base_url = f'https://api.telegram.org/bot{getenv("TGBOTKEY")}/'
channel_id = -647042555

def post_message(message: str):
    data = {
        'chat_id' : channel_id,
        'text' : message,
        'parse_mode' : 'HTML'
    }
    r = requests.post(url=base_url + 'sendMessage', data=data)
    return r.json()