from clickhouse_connect import get_client
import pandas as pd
from dotenv import load_dotenv
import os
from pathlib import Path
load_dotenv()

client = get_client(
    host='localhost',
    database='analytics',
    username='yana',
    password= os.getenv('CLICKHOUSE_PASSWORD')
)

queries = {
    "Top 5 passengers by total taxi expenses" : 
    """
    SELECT pt.passenger_first_name, pt.passenger_last_name, sum(pay.amount + pay.tips) as total_spend
        FROM passenger_table pt
        JOIN trips_table tt ON pt.passenger_id = tt.passenger_id
        JOIN payment pay ON tt.payment_id  = pay.payment_id
    GROUP BY pt.passenger_id, pt.passenger_first_name, pt.passenger_last_name
    ORDER BY total_spend  DESC
    LIMIT 5
    """,
    "Top 5 passengers by total trip duration" :
    """
    SELECT pt.passenger_first_name, pt.passenger_last_name, sum(tt.trip_duration) as total_duration
        FROM passenger_table pt
        JOIN trips_table tt ON pt.passenger_id = tt.passenger_id
        JOIN payment pay ON tt.payment_id  = pay.payment_id
    GROUP BY pt.passenger_id, pt.passenger_first_name, pt.passenger_last_name
    ORDER BY total_duration  DESC
    LIMIT 5
    """,
    "Top 5 drivers by number of trips" :
    """
    SELECT tt.driver_id, dt.driver_first_name,  dt.driver_last_name , COUNT(*) AS trips
        FROM trips_table tt
        JOIN driver_table dt ON dt.driver_id = tt.driver_id
    GROUP BY tt.driver_id, dt.driver_first_name,  dt.driver_last_name 
    ORDER BY trips DESC
    LIMIT 5

    """,
    "Trip volume by day of week" :
    """
    SELECT toDayOfWeek(pickup_datetime) AS weekday, 
	    CASE weekday
		    WHEN 1 THEN 'Monday'
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
            WHEN 7 THEN 'Sunday'
        END AS weekday_name,
    COUNT(*) AS trip_count
    FROM trips_table tt 
    GROUP BY weekday, weekday_name
    ORDER BY trip_count DESC
    """,
    "Trip volume by month" :
    """
    SELECT toStartOfMonth(pickup_datetime) AS month, 
    COUNT(*) AS trip_count
    FROM trips_table tt 
    GROUP BY month
    ORDER BY trip_count DESC
    """
}

report_dir = Path("/Users/Yana/DWH_taxi_sandbox/clickhouse/reports")
report_dir.mkdir(parents=True, exist_ok=True)

for title, sql in queries.items():
    print(f'\n{title}')
    result = client.query(sql)
    
    df = pd.DataFrame(result.result_rows, columns=result.column_names)
    print(df)
    safe_title = title.lower().replace(' ', '_')
    output_path = report_dir / f"{safe_title}.csv"
    df.to_csv(output_path, index=True)
    