import requests
import openai
import os
import csv
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("POLYGON_API_KEY")
LIMIT = 1000
url = f'https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={API_KEY}'

response = requests.get(url)
tickers = []

if response.status_code == 200:
    data = response.json()
    # guard access to results
    if isinstance(data.get('results'), list):
        for ticker in data['results']:
            tickers.append(ticker)

    # paginate safely
    while data.get('next_url'):
        next_url = data.get('next_url')
        print('Requesting next page', next_url)
        response = requests.get(next_url, params={'apiKey': API_KEY})
        if response.status_code != 200:
            print('Stopping: HTTP', response.status_code)
            break
        data = response.json()
        if not isinstance(data.get('results'), list):
            print('Stopping: page missing results or unexpected shape')
            break
        for ticker in data['results']:
            tickers.append(ticker)

    print(f"Total tickers fetched: {len(tickers)}")   

    example_ticker =  {'ticker': 'ZWS', 
    'name': 'Zurn Elkay Water Solutions Corporation', 
    'market': 'stocks', 
    'locale': 'us', 
    'primary_exchange': 'XNYS', 
    'type': 'CS', 
    'active': True, 
    'currency_name': 'usd', 
    'cik': '0001439288', 
    'composite_figi': 'BBG000H8R0N8', 	'share_class_figi': 'BBG001T36GB5', 	'last_updated_utc': '2025-09-11T06:11:10.586204443Z'} 

    output_file = 'tickers.csv'
    with open(output_file, mode = 'w',newline = '', encoding = 'utf-8') as file:
        writer = csv.DictWriter(file, fieldnames= example_ticker.keys())
        writer.writeheader()
        for t in tickers:
            row = {key: t.get(key,'') for key in example_ticker.keys()}
            writer.writerow(row)
    print(f"{len(tickers)} Tickers saved to {output_file}")
else:
    print(f"Error: {response.status_code}")



