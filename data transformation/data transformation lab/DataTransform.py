#Arlyss West
#cs410 data engineering
#data transformation lab

import pandas as pd

#1. Get data set

# Read the CSV file into a DataFrame
df = pd.read_csv('bc_trip259172515_230215.csv')

# Print the number of breadcrumb records
print(f"Number of breadcrumb records: {len(df)}")


#2. Filter
'''
#A.Use the DataFrame drop() method to filter/remove the EVENT_NO_STOP column
df = df.drop(columns=['EVENT_NO_STOP'])

# Check result
print(f"after dropping EVENT_NO_STOP column")
print(df.head())

#B. Drop the GPS_SATELLITES and GPS_HDOP columns as well.
df = df.drop(columns=['GPS_SATELLITES', 'GPS_HDOP'])

# Check result
print(f"after dropping GPS_SATELLITES and GPS_HDOP columns")
print(df.head())
'''

#C. Start over, and this time filter these same columns using the usecols parameter of the read_csv() method.  
df = pd.read_csv('bc_trip259172515_230215.csv', usecols=lambda column: column not in ['EVENT_NO_STOP', 'GPS_SATELLITES', 'GPS_HDOP'])

# Check result
print(df.head())

#3. Decode

# Define a function that will be applied to each row
def create_timestamp(row):
    opd_date = pd.to_datetime(row['OPD_DATE'], format='%d%b%Y:%H:%M:%S')
    act_time = pd.to_timedelta(row['ACT_TIME'], unit='s')
    return opd_date + act_time

# Apply the function to each row
df['TIMESTAMP'] = df.apply(create_timestamp, axis=1)

# Now drop the original OPD_DATE and ACT_TIME columns
df = df.drop(columns=['OPD_DATE', 'ACT_TIME'])

# See remaining columns
print(df.columns)

#4. Enhance
# Calculate differences
df['dMETERS'] = df['METERS'].diff()
df['dTIMESTAMP'] = df['TIMESTAMP'].diff().dt.total_seconds()

# Calculate SPEED
df['SPEED'] = df.apply(lambda row: row['dMETERS'] / row['dTIMESTAMP'] if row['dTIMESTAMP'] > 0 else 0, axis=1)

# Drop the helper columns
df = df.drop(columns=['dMETERS', 'dTIMESTAMP'])

# Check min, max, and average speed
min_speed = df['SPEED'].min()
max_speed = df['SPEED'].max()
avg_speed = df['SPEED'].mean()

print(f"Minimum SPEED: {min_speed:.2f} meters/second")
print(f"Maximum SPEED: {max_speed:.2f} meters/second")
print(f"Average SPEED: {avg_speed:.2f} meters/second")
