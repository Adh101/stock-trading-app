import requests
import openai
import os
import csv
try:
    import snowflake.connector
except Exception:
    snowflake = None
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()


def upload_to_snowflake(tickers, ds):
    """Upload a list of ticker dicts to Snowflake. Requires env vars:
    SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT, SNOWFLAKE_DATABASE
    Optional: SNOWFLAKE_SCHEMA (defaults to PUBLIC), SNOWFLAKE_TABLE (defaults to TICKERS)
    """
    push = os.getenv('PUSH_TO_SNOWFLAKE', 'false').lower() in ('1','true','yes')
    if not push:
        return
    if snowflake is None:
        print('snowflake-connector-python is not installed; skipping push')
        return

    sf_user = os.getenv('SNOWFLAKE_USER')
    sf_password = os.getenv('SNOWFLAKE_PASSWORD')
    sf_account = os.getenv('SNOWFLAKE_ACCOUNT')
    sf_database = os.getenv('SNOWFLAKE_DATABASE')
    sf_schema = os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')
    sf_table = os.getenv('SNOWFLAKE_TABLE', 'TICKERS')

    if not (sf_user and sf_password and sf_account and sf_database):
        print('Missing Snowflake credentials (SNOWFLAKE_USER/PASSWORD/ACCOUNT/DATABASE); skipping push')
        return

    conn = snowflake.connector.connect(
        user=sf_user,
        password=sf_password,
        account=sf_account,
        database=sf_database,
        schema=sf_schema,
    )
    cur = conn.cursor()
    try:
        # create table if not exists (simple schema)
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {sf_table} (
            ticker VARCHAR,
            name VARCHAR,
            market VARCHAR,
            locale VARCHAR,
            primary_exchange VARCHAR,
            type VARCHAR,
            active BOOLEAN,
            currency_name VARCHAR,
            cik VARCHAR,
            composite_figi VARCHAR,
            share_class_figi VARCHAR,
            last_updated_utc TIMESTAMP,
            ds DATE
        )"""
        cur.execute(create_sql)

        cols = '(ticker,name,market,locale,primary_exchange,type,active,currency_name,cik,composite_figi,share_class_figi,last_updated_utc,ds)'
        placeholders = ','.join(['%s'] * 13)
        insert_sql = f"INSERT INTO {sf_table} {cols} VALUES ({placeholders})"

        rows = []
        for t in tickers:
            rows.append((
                t.get('ticker'),
                t.get('name'),
                t.get('market'),
                t.get('locale'),
                t.get('primary_exchange'),
                t.get('type'),
                bool(t.get('active')),
                t.get('currency_name'),
                t.get('cik'),
                t.get('composite_figi'),
                t.get('share_class_figi'),
                t.get('last_updated_utc'),
                t.get('ds', ds),
            ))

        chunk = 500
        for i in range(0, len(rows), chunk):
            cur.executemany(insert_sql, rows[i:i+chunk])
        conn.commit()
        print(f'Pushed {len(rows)} rows to Snowflake table {sf_table}')
    finally:
        cur.close()
        conn.close()

def run_stock_job():

    API_KEY = os.getenv("POLYGON_API_KEY")
    LIMIT = 1000
    url = f'https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={API_KEY}'
    DS = datetime.now().strftime('%Y-%m-%d')
    response = requests.get(url)
    tickers = []

    if response.status_code == 200:
        data = response.json()
        # guard access to results
        if isinstance(data.get('results'), list):
            for ticker in data['results']:
                ticker['ds'] = DS
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
                ticker['ds'] = DS
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
        'composite_figi': 'BBG000H8R0N8', 	'share_class_figi': 'BBG001T36GB5', 	'last_updated_utc': '2025-09-11T06:11:10.586204443Z',
        'ds': DS} 

        output_file = 'tickers.csv'
        with open(output_file, mode = 'w',newline = '', encoding = 'utf-8') as file:
            writer = csv.DictWriter(file, fieldnames= example_ticker.keys())
            writer.writeheader()
            for t in tickers:
                row = {key: t.get(key,'') for key in example_ticker.keys()}
                writer.writerow(row)
        print(f"{len(tickers)} Tickers saved to {output_file}")
        # push to Snowflake if requested
        upload_to_snowflake(tickers, DS)
    else:
        print(f"Error: {response.status_code}")

if __name__ == "__main__":
    run_stock_job()


