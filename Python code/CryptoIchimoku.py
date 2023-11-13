#Get data from Binance
from binance import Client
from tqdm.autonotebook import tqdm
import pandas as pd
import numpy as np

def get_binance_data(ticker, interval='4h', start='1 Jan 2018', end=None):
  client = Client()
  intervals = {
      '15m': Client.KLINE_INTERVAL_15MINUTE,
      '1h':  Client.KLINE_INTERVAL_1HOUR,      
      '4h':  Client.KLINE_INTERVAL_4HOUR,
      '1d':  Client.KLINE_INTERVAL_1DAY
  }
  interval = intervals.get(interval, '4h')
#   print(f'Historical interval {interval}')
  klines = client.get_historical_klines(symbol=ticker, interval=interval, start_str=start, end_str=end)
  data = pd.DataFrame(klines)
  data.columns = ['open_time','open', 'high', 'low', 'close', 'volume','close_time', 'qav','num_trades','taker_base_vol','taker_quote_vol', 'ignore']
  data.index = [pd.to_datetime(x, unit='ms').strftime('%Y-%m-%d %H:%M:%S') for x in data.open_time]
  usecols=['open', 'high', 'low', 'close', 'volume', 'qav','num_trades','taker_base_vol','taker_quote_vol']
  data = data[usecols]
  data = data.astype('float')
  return data


client = Client()
exchange_info = client.get_exchange_info()
symbols=[s['symbol'] for s in exchange_info['symbols'] if s['status'] == 'TRADING']
print(symbols)
ticker_list = [d for d in symbols if 'USDT' in d]
# tiker_list = np.random.choice(symbols, size=50)
print('Number of crypto pairs: ', len(ticker_list))
print('USDT pairs: ', *ticker_list)

# collect pair closes in one dataframe
Final_data = pd.DataFrame()
for ticker in tqdm(ticker_list):
    try:
        e = pd.DataFrame(get_binance_data(ticker, interval='1d', start='1 Jan 2018'))
        e['Crypto'] = ticker
        Final_data = pd.concat([Final_data,e])
    except Exception as err:
        print(err)
        continue

# print(coins.head())
Final_data =  Final_data.reset_index()
Final_data = Final_data.rename(columns = {'index':'Date'})
Final_data = Final_data.sort_values(by = ['Crypto','Date'])

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

df = Final_data
# Convert the 'Date' column to datetime
df['Date'] = pd.to_datetime(df['Date'])

# Find the latest date
latest_date = df['Date'].max()

# Filter the DataFrame for the latest date
latest_data = df[df['Date'] == latest_date]

# Sort the DataFrame by 'Momentum_Week' and 'Momentum_Daily' columns in descending order
top_10_month = latest_data.sort_values(by='Momentum_Month', ascending=False).head(10)
top_10_week = latest_data.sort_values(by='Momentum_Week', ascending=False).head(10)
top_10_daily = latest_data.sort_values(by = 'Momentum_Daily' , ascending =False).head(10)

# Select only the desired columns
top_10_month = top_10_month[['Crypto', 'Momentum_Month']]
top_10_week = top_10_week[['Crypto', 'Momentum_Week']]
top_10_daily = top_10_daily[['Crypto', 'Momentum_Daily']]

# Reset index for the resulting DataFrames
top_10_month.reset_index(drop=True, inplace=True)
top_10_week.reset_index(drop=True, inplace=True)
top_10_daily.reset_index(drop=True, inplace=True)

df = df.sort_values(by=['Crypto', 'Date'])

# Calculate the 'Sign' value for the previous date
df['Previous_Sign'] = df.groupby('Crypto')['Sign'].shift(1)

# Filter for Bullish crypto with the latest date 'Sign' as 'Bullish' and yesterday 'Sign' as 'No Sign'
bullish_crypto = df.loc[(df['Sign'] == 'Bullish') & (df['Previous_Sign'] == 'No sign')]
bullish_crypto = bullish_crypto.loc[bullish_crypto['Date'] == latest_date]
# Filter for Bearish crypto with the latest date 'Sign' as 'Bearish' and yesterday 'Sign' as 'No Sign'
bearish_crypto = df[(df['Sign'] == 'Bearish') & (df['Previous_Sign'] == 'No sign')]
bearish_crypto = bearish_crypto.loc[bearish_crypto['Date'] == latest_date]
# Get the unique list of Bullish and Bearish cryptocurrencies
unique_bullish_crypto = bullish_crypto['Crypto'].unique()
unique_bearish_crypto = bearish_crypto['Crypto'].unique()

print("Bullish Cryptos:")
print(unique_bullish_crypto)

print("\nBearish Cryptos:")
print(unique_bearish_crypto)

# Print the top 10 Momentum_Week and top 10 Momentum_Daily
print("Top 10 Momentum_Month:")
print(top_10_month)

print("\nTop 10 Momentum_Week:")
print(top_10_week)

print("\nTop 10 Momentum_Daily:")
print(top_10_daily)

#Bot Telegram
from telegram import Bot

# Assuming you have a DataFrame called df and have filtered the cryptocurrencies as per your previous question

import telebot

# Replace 'YOUR_BOT_TOKEN' and 'YOUR_CHAT_ID' with your bot token and chat ID
bot_token = '6571668753:AAEZE34YTHC0YGJFuM9-md8CZZZRYBHw60s'
chat_id = '854720471'

# Initialize the bot
bot = telebot.TeleBot(bot_token)
latest_date_str = latest_date.strftime("%Y-%m-%d %H:%M:%S")
# Format the data into a single message
message = f"Latest Date: {latest_date_str}\n\n"
message += f"Unique Bullish Crypto:\n{', '.join(unique_bullish_crypto)}\n\n"
message += f"Unique Bearish Crypto:\n{', '.join(unique_bearish_crypto)}\n\n"

# DataFrames
message += "Top 10 Momentum_Month:\n" + top_10_month.to_string(index=False) + "\n\n"
message += "Top 10 Momentum_Week:\n" + top_10_week.to_string(index=False) + "\n\n"
message += "Top 10 Momentum_Daily:\n" + top_10_daily.to_string(index=False)

# Send the formatted message to your bot
bot.send_message(chat_id, message)

