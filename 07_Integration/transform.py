import pandas as pd

# Load COVID data from CSV into a DataFrame
covid_data = pd.read_csv('/home/bhu/COVID_county_data.csv')

# Load census tract data from CSV into a DataFrame
census_data = pd.read_csv('/home/bhu/acs2017_census_tract_data.csv')

# Step 1: Transform COVID data to monthly level
covid_data['date'] = pd.to_datetime(covid_data['date'])  # Convert date column to datetime format
covid_data['Month'] = covid_data['date'].dt.to_period('M')  # Extract month from date
covid_monthly = covid_data.groupby(['county', 'state', 'Month']).agg({'cases': 'sum', 'deaths': 'sum'}).reset_index()

# Step 2: Add county ID as a foreign key lookup to the County_Info DataFrame
# Merge county_info DataFrame into COVID data
county_info = census_data.groupby(['County', 'State']).agg({
    'TotalPop': 'sum',
    'Poverty': 'mean',
    'IncomePerCap': 'mean'
}).reset_index()
county_info['ID'] = range(1, len(county_info) + 1)

covid_monthly = pd.merge(covid_monthly, county_info, how='left', left_on=['county', 'state'], right_on=['County', 'State'])
covid_monthly.drop(columns=['County', 'State'], inplace=True)

# Step 3: Fill in the table for Malheur County, Oregon
malheur_table = covid_monthly[(covid_monthly['county'] == 'Malheur') & (covid_monthly['state'] == 'Oregon')]
print("Table for Malheur County, Oregon:")
print(malheur_table[['county', 'state', 'Month', 'cases', 'deaths']])