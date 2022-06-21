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
    return json.loads(r.content)

def post_image(message: str, image: str):
    data = {
        'chat_id' : channel_id,
        'caption' : message,
        'parse_mode' : 'HTML',
    }

    files = {
        'photo' : open(image, 'rb')
    }

    r = requests.post(url=base_url + 'sendPhoto', files=files, data=data)
    return json.loads(r.content)