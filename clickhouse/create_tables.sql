CREATE TABLE IF NOT EXISTS analytics.trips_table (
id String,
passenger_id UInt32,
driver_id UInt32,
vendor_id UInt8,
payment_id String,
payment_method_id UInt8, 
pickup_datetime DateTime,
dropoff_datetime DateTime,
trip_duration UInt16,
passenger_count UInt8,
pickup_longitude Float64,
pickup_latitude Float64,
dropoff_longitude Float64,
dropoff_latitude Float64,
score Nullable(UInt8)
)
ENGINE = MergeTree
ORDER BY pickup_datetime;


CREATE TABLE IF NOT EXISTS analytics.passenger_table (
passenger_id UInt32,
passenger_first_name String,
passenger_last_name String,
passenger_age UInt8,
passenger_rating UInt8
)
ENGINE = MergeTree
ORDER BY passenger_id;


CREATE TABLE IF NOT EXISTS analytics.driver_table (
driver_id UInt32,
driver_first_name String,
driver_last_name String,
driver_rating UInt8
)
ENGINE = MergeTree
ORDER BY driver_id;


CREATE TABLE IF NOT EXISTS analytics.driver_vehicle (
vehicle_id UInt32,
driver_id UInt32,
plate_number String,
color_id UInt8
)
ENGINE = MergeTree
ORDER BY vehicle_id;


CREATE TABLE IF NOT EXISTS analytics.vehicle_info (
vehicle_id UInt32,
make String,
model String,
vehicle_year UInt32
)
ENGINE = MergeTree
ORDER BY vehicle_id;


CREATE TABLE IF NOT EXISTS analytics.car_color (
color_id UInt8,
color_name String
)
ENGINE = MergeTree
ORDER BY color_id;


CREATE TABLE IF NOT EXISTS analytics.vendor (
vendor_id UInt8,
company_name String
)
ENGINE = MergeTree
ORDER BY vendor_id;


CREATE TABLE IF NOT EXISTS analytics.payment (
payment_id String,
payment_method_id UInt8,
amount Float64,
tips Float64
)
ENGINE = MergeTree
ORDER BY payment_id;


CREATE TABLE IF NOT EXISTS analytics.payment_method (
payment_method_id UInt8,
payment_method_name String
)
ENGINE = MergeTree
ORDER BY payment_method_id;