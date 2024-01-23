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

theFile = input("Congratulations on making it to another bank statement. If you deleted the six poop rows at the beginning and that one beginning balance row and you saved it to the level1 folder, you can tell me the yyyy_mm it is then I will load it into your finance database swiftly: ")

df = pd.read_csv(f'/Users/aferg/data/finance/statement_downloads/level1/{theFile}.csv')

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
# Create a session (could make this into a separate script defining as function)
# Session = sessionmaker(bind=engine)
# session = Session()

# Base = declarative_base()

# define any number of mapped classes in terms of the Base
# class december(Base):
#     __tablename__ = 'december'
# 
#     date = Column(Date)
#     description = Column(String)
#     amount = Column(Integer)
#     running_bal = Column(Integer)
#     id = Column(Integer, primary_key=True)

#insert data into table
# table_entry = december(
# date=df['date']
# , description=df['description']
# , amount=df['amount']
# , running_bal=df['running_bal']
# , id = df['id']
# )
# session.add(table_entry)

# Commit the changes to the database
# session.commit()

#close the session
# session.close()
