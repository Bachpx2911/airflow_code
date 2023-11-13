#Library
from datetime import datetime, timedelta
from binance import Client
from tqdm.autonotebook import tqdm
import pandas as pd
import numpy as np
import pandas as pd
import hashlib
import psycopg2
from dotenv import dotenv_values

#define get_binance_data function
def get_binance_data(ticker, interval='4h', start='1 Jan 2018', end=None):
  client = Client()
  intervals = {
      '15m': Client.KLINE_INTERVAL_15MINUTE,
      '1h':  Client.KLINE_INTERVAL_1HOUR,      
      '4h':  Client.KLINE_INTERVAL_4HOUR,
      '1d':  Client.KLINE_INTERVAL_1DAY
  }
  interval = intervals.get(interval, '4h')
  klines = client.get_historical_klines(symbol=ticker, interval=interval, start_str=start, end_str=end)
  data = pd.DataFrame(klines)
  data.columns = ['open_time','open', 'high', 'low', 'close', 'volume','close_time', 'qav','num_trades','taker_base_vol','taker_quote_vol', 'ignore']
  data.index = [pd.to_datetime(x, unit='ms').strftime('%Y-%m-%d %H:%M:%S') for x in data.open_time]
  usecols=['open', 'high', 'low', 'close', 'volume', 'qav','num_trades','taker_base_vol','taker_quote_vol']
  data = data[usecols]
  data = data.astype('float')
  return data

#define start_date
config = dotenv_values(".env") 
pg_host = config['PGHOST'] 
pg_user = config['PGUSER']
pg_password = config['PGPASSWORD']
pg_database = config['PGDATABASE']
conn = psycopg2.connect(
    host=pg_host,
    user=pg_user,
    password=pg_password,
    database=pg_database
)
cur = conn.cursor()
query = '''
        SELECT TO_CHAR(cast(max("Date") as date) - INTERVAL '1 day','DD Mon YYYY') as Date_var
        FROM public.binance_data;
        '''
cur.execute(query)
version = cur.fetchall()
start_date = version[0][0]

#Get list crypto
client = Client()
exchange_info = client.get_exchange_info()
symbols=[s['symbol'] for s in exchange_info['symbols'] if s['status'] == 'TRADING']
print(symbols)
ticker_list = [d for d in symbols if 'USDT' in d]

# collect pair closes in one dataframe
Final_data = pd.DataFrame()
for ticker in tqdm(ticker_list):
    try:
        e = pd.DataFrame(get_binance_data(ticker, interval='1d', start=start_date))
        e['Crypto'] = ticker
        Final_data = pd.concat([Final_data,e])
    except Exception as err:
        print(err)
        continue

# print(coins.head())
Final_data =  Final_data.reset_index()
Final_data = Final_data.rename(columns = {'index':'Date'})
Final_data = Final_data.sort_values(by = ['Crypto','Date'])

# Create a hash key based on 'Date' and 'Crypto' columns
def create_hash_key(row):
    data_to_hash = f"{row['Date']}_{row['Crypto']}"
    return hashlib.md5(data_to_hash.encode()).hexdigest()

Final_data['Primary Key'] = Final_data.apply(create_hash_key, axis=1)

# Function to calculate the Ichimoku lines
def compute_ichimoku(df):
    df['Tenkan_Sen'] = (df['high'].rolling(window=9).max() + 
                        df['low'].rolling(window=9).min()) / 2

    df['Kijun_Sen'] = (df['high'].rolling(window=26).max() + 
                       df['low'].rolling(window=26).min()) / 2
    df['Momentum_Month'] = df['close'].pct_change(periods=30) * 100
    df['Momentum_Week'] = df['close'].pct_change(periods=7) * 100
    return df

# Apply the function to each unique crypto
Final_data = Final_data.groupby('Crypto').apply(compute_ichimoku)

# Function to calculate 'Sign'
def calculate_sign(row):
    if row['Tenkan_Sen'] > row['Kijun_Sen'] and row['Momentum_Month'] > 0 :
        return 'Bullish'
    elif row['Tenkan_Sen'] < row['Kijun_Sen'] and row['Momentum_Month'] < 0 :
        return 'Bearish'
    else:
        return 'No sign'

# Apply the function to each row and create the 'Sign' column
Final_data['Sign'] = Final_data.apply(calculate_sign, axis=1)
Final_data['Momentum_Daily'] = (Final_data['close'] - Final_data['open']) / Final_data['open'] * 100
#Timezone binance : 7h sáng vnam đóng nến ngày trên binance

#Write file to the output
output_file = r'C:/Users/ADMIN/Desktop/Data_Crypto/3RecentDays_price.csv'
Final_data.to_csv(output_file,index = False)


