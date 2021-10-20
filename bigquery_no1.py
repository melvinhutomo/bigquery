from google.cloud import bigquery
from datetime import timedelta
import pandas as pd

client = bigquery.Client()

sql = """
    SELECT *
    FROM `test-melvin-329603.samples.date_data`
"""

df = client.query(sql).to_dataframe()
df['start_time_new']=df['start_time'].dt.round('15min')
df['end_time_new']=df['end_time'].dt.round('15min')
start_time_new = df['start_time_new'][0]
end_time_new = df['end_time_new'][0]

start_time = df['start_time'][0]
end_time = df['end_time'][0]

def datetime_range(start, end, delta):
    current = start
    while current < end:
        yield current
        current += delta

dts = [dt.strftime('%Y-%m-%d T%H:%M Z') for dt in 
       datetime_range(start_time_new, end_time_new, 
       timedelta(minutes=30))]

dts.insert(0, start_time)
dts.append(end_time)
print(dts)
