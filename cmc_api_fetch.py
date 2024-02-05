import os
from dotenv import load_dotenv

from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json

load_dotenv()

URL = 'https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest'
API_KEY = os.getenv("CMC_API_KEY")
PARAMS = {
  'symbol':'BTC',
}

HEADERS = {
  'Accepts': 'application/json',
  'X-CMC_PRO_API_KEY': API_KEY,
}

session = Session()
session.headers.update(HEADERS)

try:
  response = session.get(URL, params=PARAMS)
  data = json.loads(response.text)
  print(data)
except (ConnectionError, Timeout, TooManyRedirects) as e:
  print(e)