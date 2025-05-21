import pandas as pd
import numpy as np
import random
import string
from datetime import datetime, timedelta


def generate_payment_ids(n=1):
    ids = set()
    while len(ids) < n:
        part1 = ''.join(random.choices(string.ascii_uppercase, k=3))
        part2 = ''.join(random.choices(string.digits, k=4))
        payment_id = 'PAY-' + part1 + part2
        ids.add(payment_id)
    if n == 1:
        return list(ids)[0]
    return list(ids)


def generate_plate_number(n=1):
    numbers = set()
    while len(numbers) < n:
        part1 = ''.join(random.choices(string.ascii_uppercase, k=1))
        part2 = ''.join(random.choices(string.digits, k=2))
        part3 = ''.join(random.choices(string.ascii_uppercase, k=2))
        part4 = ''.join(random.choices(string.digits, k=1))
        plate_number = part1 + part2 + part3 + part4
        numbers.add(plate_number)
    if n == 1:
        return list(numbers)[0]
    return list(numbers)


def generate_int_range(start, end, volume=1):
    if volume == 1:
        return int(np.random.randint(start, end, size=1))
    return np.random.randint(start, end, size=volume)
    
 
def generate_trips_ids(df):
    df_last_id = pd.read_csv('/Users/Yana/DWH_taxi_sandbox/synthetic_df_output/synth.csv')
    last_num = df_last_id['id'].iloc[-1]
    return 'id' + str(int(last_num[2:])+1)
    

def generate_location(dim):
    if dim == 'lat':
        return round(np.random.uniform(40.70, 40.88), 6)
    if dim == 'lon':
        return round(np.random.uniform(-74.02, -73.93), 6)
    

def generate_time():
    duration = np.random.randint(6, 115)
    end = datetime.now()
    start = end - timedelta(minutes= duration)
    return [start, end, duration]


def if_exists(value, table_name, column, client):

    if isinstance(value, list) and len(value) == 1:
        value = value[0]

    val = f"'{value}'" if isinstance(value, str) else value
    query = f'SELECT count() FROM {table_name} WHERE {column} = {val}'
    return client.query(query).result_rows[0][0] > 0