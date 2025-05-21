
import pandas as pd
import numpy as np
import random
from faker import Faker
from Generation_func import generate_plate_number, generate_int_range, generate_payment_ids, generate_time, generate_location, if_exists
from car_info import car_models, car_colors
from clickhouse_connect import get_client
fake = Faker()

def clickhouse_insert(client, table_name, info):
    print(f'Inserting into {table_name}: {info}')
    df = pd.DataFrame([info])

    client.insert_df(table=table_name, df=df)

def generate_trip(host, database, username, password, **kwargs):
    client = get_client(
        host=host,
        database=database,
        username=username,
        password=password
    )
    new_pass = generate_int_range(100, 10000)
    print(f"DEBUG new_pass = {new_pass} ({type(new_pass)})")
    if not if_exists(new_pass, 'passenger_table', 'passenger_id', client):
        new_pass_info = {
            'passenger_id' : new_pass,
            'passenger_first_name' : fake.first_name(),
            'passenger_last_name' : fake.last_name(),
            'passenger_age' : generate_int_range(18,100),
            'passenger_raiting' : generate_int_range(1,6)
        }
        clickhouse_insert(client, 'passenger_table', new_pass_info)


    new_driver = generate_int_range(100, 10000)
    if not if_exists(new_driver, 'driver_table', 'driver_id', client):
        new_driver_info = {
            'driver_id' : new_driver,
            'driver_first_name' : fake.first_name(),
            'driver_last_name' : fake.last_name(),
            'driver_rating' : generate_int_range(1,6)
        }
        clickhouse_insert(client, 'driver_table', new_driver_info)

        max_vehicle_id = client.query("SELECT max(vehicle_id) FROM driver_vehicle").result_rows
        new_vehicle_id = max_vehicle_id[0][0] + 1
        new_driver_vehicle_info = {
            'vehicle_id' : new_vehicle_id,
            'driver_id' : new_driver,
            'plate_number' : generate_plate_number(),
            'color_id' : generate_int_range(1,14)
        }
        clickhouse_insert(client, 'driver_vehicle', new_driver_vehicle_info)

        make = np.random.choice(list(car_models.keys()))
        model = np.random.choice(car_models[make])
        new_vehicle_info = {
            'vehicle_id' : new_driver_vehicle_info['vehicle_id'],
            'make' : make,
            'model' : model,
            'vehicle_year' : generate_int_range(1960, 2026)
        }
        clickhouse_insert(client, 'vehicle_info', new_vehicle_info)

    new_payment_method_id = generate_int_range(1,7)
    new_amount = np.round(np.random.uniform(20.0, 120.0), 2)
    tip_percentages = [0.03, 0.05, 0.07, 0.10]
    tips_percent = np.random.choice(tip_percentages)
    new_payment_id = generate_payment_ids()[0]
    payment_info = {
        'payment_id' : new_payment_id,
        'payment_method_id' : new_payment_method_id,
        'amount' : new_amount,
        'tips' : float(np.round(new_amount * tips_percent, 2))
    }
    clickhouse_insert(client, 'payment', payment_info)

    pickup_dt, dropoff_dt, duration = generate_time()

    last_trip_id = client.query("SELECT id FROM trips_table ORDER BY id DESC LIMIT 1").result_rows
    last = last_trip_id[0][0]
    last_num = int(last.replace('id', ''))
    new_id = f'id{last_num + 1:07}'

    trips_info = {
        'id' : new_id,
        'passenger_id' : new_pass,
        'driver_id' : new_driver,
        'vendor_id' : generate_int_range(1,21),
        'payment_id' : new_payment_id,
        'pickup_datetime' : pickup_dt,
        'dropoff_datetime' : dropoff_dt,
        'trip_duration' : duration,
        'passenger_count' : generate_int_range(1,6),
        'pickup_longitude' : generate_location('lon'),
        'pickup_latitude' : generate_location('lat'),
        'dropoff_longitude' : generate_location('lon'),
        'dropoff_latitude' : generate_location('lat'),
        'score' : generate_int_range(1,6)
    }
    clickhouse_insert(client, 'trips_table', trips_info)

    print('Trip successfully created')
    print(f"Trip ID : {trips_info['id']}")
    print(f"Passenger ID : {trips_info['passenger_id']}")
    print(f"Driver ID : {trips_info['driver_id']}")
    print(f"Duration : {trips_info['trip_duration']} minutes")

