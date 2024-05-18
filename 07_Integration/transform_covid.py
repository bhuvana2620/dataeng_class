import pandas as pd

# Load COVID data from CSV into a DataFrame
covid_data = pd.read_csv('/home/bhu/COVID_county_data.csv')

# Load census tract data from CSV into a DataFrame
census_data = pd.read_csv('/home/bhu/acs2017_census_tract_data.csv')

# Aggregate census tract data to get demographic summaries for each county
county_info = census_data.groupby(['County', 'State']).agg({
    'TotalPop': 'sum',
    'Poverty': 'mean',
    'IncomePerCap': 'mean'
}).reset_index()

# Add unique integer IDs for the counties
county_info['ID'] = range(1, len(county_info) + 1)

# Add county ID for each county in the COVID data
covid_data['ID'] = covid_data['county'] + ', ' + covid_data['state']

# Merge county_info DataFrame into COVID data
covid_data = pd.merge(covid_data, county_info, how='left', left_on=['county', 'state'], right_on=['County', 'State'])

# Drop one of the duplicated 'ID' columns
covid_data.drop(columns=['ID_y'], inplace=True)

# Transform COVID data to monthly level
covid_data['date'] = pd.to_datetime(covid_data['date'])  # Convert date column to datetime format
covid_data['Month'] = covid_data['date'].dt.to_period('M')  # Extract month from date

# Group by 'ID' and 'Month' and aggregate cases and deaths
covid_monthly = covid_data.groupby(['ID_x', 'Month']).agg({'cases': 'sum', 'deaths': 'sum'}).reset_index()

# Print County_Info DataFrame
print("County_Info DataFrame:")
print(county_info)

# Print COVID_monthly DataFrame
print("\nCOVID_monthly DataFrame:")
print(covid_monthly)

# Check if Malheur County exists in County_Info DataFrame
print("\nCounty names in County_Info DataFrame:")
print(county_info['County'].unique())

# Check if Malheur County exists in COVID_monthly DataFrame
print("\nCounty names in COVID_monthly DataFrame:")
print(covid_monthly['ID_x'].apply(lambda x: x.split(',')[0].strip()).unique())

# Check if there is data available for Malheur County, Oregon, for specified months
print("\nData availability for Malheur County, Oregon, for specified months:")
malheur_data = covid_monthly[covid_monthly['ID_x'].str.startswith('Malheur')]
months_to_check = ['2020-08', '2021-01', '2021-02']

for month in months_to_check:
    data_available = malheur_data[malheur_data['Month'] == month]
    if not data_available.empty:
        print(f"Data available for Malheur County, Oregon, in {month}:")
        print(data_available)
    else:
        print(f"No data available for Malheur County, Oregon, in {month}.")
