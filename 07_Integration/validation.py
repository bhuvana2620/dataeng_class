import pandas as pd

# Load COVID data
covid_data = pd.read_csv('COVID_county_data.csv')

# Load ACS data
acs_data = pd.read_csv('acs2017_census_tract_data.csv')

# Check unique county names in COVID data
covid_county_names = covid_data['county'].unique()

# Check unique county names in ACS data
acs_county_names = acs_data['County'].unique()

# Find county names present in COVID data but not in ACS data
missing_county_names = set(covid_county_names) - set(acs_county_names)

print("County names present in COVID data but not in ACS data:")
print(missing_county_names)

# Find county names present in ACS data but not in COVID data
extra_county_names = set(acs_county_names) - set(covid_county_names)

print("\nCounty names present in ACS data but not in COVID data:")
print(extra_county_names)