import pandas as pd
import numpy as np
import random
from faker import Faker
from Generation_func import generate_plate_number, generate_int_range, generate_payment_ids
from car_info import car_models, car_colors

df = pd.read_csv('/Users/Yana/DWH_taxi_sandbox/raw_sources/NYC.csv')
df = df.drop('store_and_fwd_flag', axis=1)

#trips_table
df['driver_id'] = generate_int_range(100, 1000, len(df))
df['vendor_id'] = generate_int_range(1,21, len(df))
df['payment_id'] = generate_payment_ids(len(df))
df['payment_method_id'] = generate_int_range(1,7, len(df))
df['score'] = generate_int_range(1,6, len(df))

mask = np.random.rand(len(df)) < 0.3
df.loc[mask, 'score'] = np.nan

#vendor_table
fake = Faker()
vendor_ids = df['vendor_id'].unique()
vendor_to_company = {vendor_id : fake.company() for vendor_id in vendor_ids}
df['company_name'] = df['vendor_id'].map(vendor_to_company)

#payment_table
df['amount'] = np.round(np.random.uniform(20.0, 120.0, size=len(df)), 2)

tip_percentages = [0.03, 0.05, 0.07, 0.10]
tips_percent = np.random.choice(tip_percentages, size= len(df))
df['tips'] = np.round(df['amount'] * tips_percent, 2)

payment_methods = ['cash', 'card', 'google pay', 'apple pay', 'check', 'zelle']

unique_payment_method_id = df['payment_method_id'].unique()
dim_payment_method = pd.DataFrame({
    'payment_method_id' : unique_payment_method_id,
    'payment_method_name' : payment_methods
})

df = df.merge(dim_payment_method, on= 'payment_method_id', how='left')

#passenger_table
number_of_passenger = len(df) // 5
passenger_ids = np.random.choice(np.arange(100, 10000), size=number_of_passenger)

all_passenger_ids = []
for id in passenger_ids:
    repeats = random.randint(4,15)
    all_passenger_ids.extend([id] * repeats)

all_passenger_ids = all_passenger_ids[:len(df)]
random.shuffle(all_passenger_ids)
df['passenger_id'] = all_passenger_ids

unique_passengers = df['passenger_id'].unique()

young = np.random.randint(18, 36, size=int(len(unique_passengers) * 0.5))
adult = np.random.randint(36, 56, size=int(len(unique_passengers) * 0.4))
seniors = np.random.randint(56, 101, size=int(len(unique_passengers) * 0.1))
ages = np.concatenate([young, adult, seniors])
np.random.shuffle(ages)

dim_passenger = pd.DataFrame({
    'passenger_id' : unique_passengers,
    'passenger_first_name' : [fake.first_name() for _ in range(len(unique_passengers))],
    'passenger_last_name' : [fake.last_name() for _ in range(len(unique_passengers))],
    'passenger_age' : ages,
    'passenger_rating' : generate_int_range(1,6, len(unique_passengers))
})

df = df.merge(dim_passenger, on = 'passenger_id', how='left')

#driver_table
number_of_drivers = len(df) // 100
driver_ids = np.random.choice(np.arange(100, 10000), size=number_of_drivers)

all_driver_ids = []
for id in driver_ids:
    repeats = random.randint(50, 400)
    all_driver_ids.extend([id] * repeats)

all_driver_ids = all_driver_ids[:len(df)]
random.shuffle(all_driver_ids)
df['driver_id'] = all_driver_ids

unique_d = df['driver_id'].unique()

make_list = np.random.choice(list(car_models.keys()), size=len(unique_d))
model_list = [random.choice(car_models[make]) for make in make_list]

dim_driver = pd.DataFrame({
    'driver_id' : unique_d,
    'driver_first_name' : [fake.first_name() for _ in range(len(unique_d))],
    'driver_last_name' : [fake.last_name() for _ in range(len(unique_d))],
    'driver_rating' : generate_int_range(1,6, len(unique_d)),
    'vehicle_id' : generate_int_range(1000, 100000, len(unique_d)),
    'plate_number' : generate_plate_number(len(unique_d)),
    'color_id' : generate_int_range(1,14, len(unique_d)),
    'make' : make_list,
    'model' : model_list,
    'vehicle_year' : generate_int_range(1960, 2026, len(unique_d))
})

df = df.merge(dim_driver, on = 'driver_id', how='left')

unique_color_ids = df['color_id'].unique()
dim_color = pd.DataFrame({
    'color_id' : unique_color_ids,
    'color_name' : np.random.choice(car_colors, size=len(unique_color_ids))
})
df = df.merge(dim_color, on = 'color_id', how = 'left')

df.to_csv('/Users/Yana/DWH_taxi_sandbox/synthetic_df_output/synth.csv', index=False)

def drop_dup(df):
    return df.drop_duplicates().reset_index(drop=True)

trips_table = df[['id', 'passenger_id', 'driver_id', 'vendor_id', 'payment_id', 'payment_method_id', 'pickup_datetime', 'trip_duration', 'passenger_count', 'pickup_longitude', 'dropoff_longitude', 'pickup_latitude', 'dropoff_latitude', 'score']]
passenger_table = df[['passenger_id', 'passenger_first_name', 'passenger_last_name', 'passenger_age', 'passenger_rating']]
driver_table = df[['driver_id', 'driver_first_name', 'driver_last_name', 'driver_rating']]
driver_vehicle_table = df[['vehicle_id', 'driver_id', 'plate_number', 'color_id']]
car_color_table = df[['color_id', 'color_name']]
vehicle_info_table = df[['vehicle_id', 'make', 'model', 'vehicle_year']]
vendor_table = df[['vendor_id', 'company_name']]
payment_table = df[['payment_id', 'payment_method_id', 'amount', 'tips']]
payment_method_table = df[['payment_method_id', 'payment_method_name']]

tables = [
    ('trips_table', trips_table),
    ('passenger_table', passenger_table),
    ('driver_table', driver_table),
    ('driver_vehicle_table', driver_vehicle_table),
    ('car_color_table', car_color_table),
    ('vehicle_info_table', vehicle_info_table),
    ('vendor_table', vendor_table),
    ('payment_table', payment_table),
    ('payment_method_table', payment_method_table)
]
for name, table in tables:
    table = drop_dup(table)
    table = table.sort_values(by= table.columns[0], ascending=True)
    table.to_csv(f'/Users/Yana/DWH_taxi_sandbox/synthetic_df_output/{name}.csv', index= False)