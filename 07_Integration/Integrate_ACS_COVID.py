import pandas as pd

# Define the file paths
covid_data_path = 'COVID_county_data.csv'
acs_data_path = 'acs2017_census_tract_data.csv'

# Define chunk size for stream processing
chunk_size = 1000

# Stream processing for COVID data
covid_chunks = pd.read_csv(covid_data_path, chunksize=chunk_size)
covid_data_sample = pd.concat([chunk.sample(frac=0.1) for chunk in covid_chunks])  # Sample 10% of data

# Stream processing for ACS data
acs_chunks = pd.read_csv(acs_data_path, chunksize=chunk_size)
acs_data_sample = pd.concat([chunk.sample(frac=0.1) for chunk in acs_chunks])  # Sample 10% of data

# Process 'County' column in ACS data
acs_data_sample['County'] = acs_data_sample['County'].str.replace(' County', '')

# Ensure state columns are in the same format
covid_data_sample['state'] = covid_data_sample['state'].str.strip()
acs_data_sample['State'] = acs_data_sample['State'].str.strip()

# Merge COVID and ACS data on the 'county' and 'state' columns
merged_data = pd.merge(covid_data_sample, acs_data_sample, how='inner', left_on=['county', 'state'], right_on=['County', 'State'])

# Display the merged data
print("Merged Data:")
print(merged_data.head())

# Generate summary statistics for COVID data
COVID_summary = covid_data_sample.groupby(['county', 'state']).agg({
    'cases': 'sum',
    'deaths': 'sum'
}).reset_index()

# Check if COVID summary is empty
if COVID_summary.empty:
    print("COVID summary is empty.")
else:
    print("\nCOVID Summary DataFrame:")
    print(COVID_summary.head())

    # Example: Print summary statistics for specific counties
    counties_of_interest = [
        ('Washington', 'Oregon'),
        ('Malheur', 'Oregon'),
        ('Loudoun', 'Virginia'),
        ('Harlan', 'Kentucky')
    ]
    print("\nCounty | State | Total Cases | Total Deaths")
    for county, state in counties_of_interest:
        county_data = COVID_summary[(COVID_summary['county'] == county) & (COVID_summary['state'] == state)]
        if not county_data.empty:
            total_cases = county_data['cases'].iloc[0]
            total_deaths = county_data['deaths'].iloc[0]
            print(f"{county}, {state}   | {total_cases}   | {total_deaths}")
        else:
            print(f"No data available for {county}, {state}")

    # Example: Accessing data directly for Washington County, Oregon
    county_data = COVID_summary[(COVID_summary['county'] == 'Washington') & (COVID_summary['state'] == 'Oregon')]
    if not county_data.empty:
        print("\nCounty | State | Total Cases | Total Deaths")
        print("Washington, Oregon   | ", county_data['cases'].iloc[0], "   | ", county_data['deaths'].iloc[0])
    else:
        print("No data available for Washington, Oregon")