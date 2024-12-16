from os import path
import pandas as pd
from datetime import date
import requests
from sqlalchemy import create_engine, Column, String, Integer, Date
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base
import json

# load config
with open('/Users/aferg/garden/finance_loader/configuration.json', 'r') as file:
    config = json.load(file)


theFolder = input("The process starts with the folder that corresponds with the source of finances (ie 'chase', 'bOfA'): ")
print(theFolder)

theFile = input("Delete the six rows at the beginning and that one beginning balance row, saved it to the level1 folder, then enter the yyyy_mm: ")

df = pd.read_csv(f'/Users/aferg/data/finance/statement_downloads/level1/{theFolder}/{theFile}.csv')

# the newly added chase method
if theFolder =='chase':
    # modify df
    df = df.rename(columns={"Post Date": "date", "Description": "description", "Category": "category", "Amount": "amount", "Memo": "memo"}, errors="raise")
    df['date'] = pd.to_datetime(df['date']).dt.date
    print(df.dtypes)
    df['amount'] = df['amount'].abs()
    df = df[['date', 'description', 'category', 'amount']]
    print(df.head())

# else do the bOfA method
else:
    # modify df
    df = df.rename(columns={"Date": "date", "Description": "description", "Amount": "amount", "Running Bal.": "running_bal"}, errors="raise")
    df['date'] = pd.to_datetime(df['date'])
    df['date'] = df['date'].dt.date
    df['amount'] = df['amount'].str.strip('-')
    df['amount'] = df['amount'].replace(',','', regex=True)
    df['amount'] = df['amount'].astype(float)
    df['running_bal'] = df['running_bal'].str.strip('-')
    df['running_bal'] = df['running_bal'].replace(',','', regex=True)
    df['running_bal'] = df['running_bal'].astype(float)
    print(df.head())

# header - user - password - tailer - db
database_url = config['header']  + config['user'] + config['password'] + config['tailer'] + config['db'] 
engine = create_engine(database_url)

df.to_sql(theFile, engine)
