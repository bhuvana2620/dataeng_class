mport pandas as pd

# 1. Load ACS data
acs_data = pd.read_csv('/home/bhu/acs2015_census_tract_data_part1.csv')

# 2. Aggregate Data
county_summary = acs_data.groupby(['County', 'State']).agg({
    'TotalPop': 'sum',
    'Poverty': 'mean',
    'IncomePerCap': 'mean'
}).reset_index()

# 3. Create County_info DataFrame
county_info = county_summary.rename(columns={'County': 'Name'})
county_info['ID'] = range(1, len(county_info) + 1)  # Generating sequential IDs

# Check if 'ID' column is unique
is_unique = county_info['ID'].is_unique
print("Is 'ID' column unique?", is_unique)

# 4. Assign Unique IDs (if necessary)
# Assuming 'ID' column is already unique

# 5. Fill in Sample Data
sample_data = {
    'Name': ['County A', 'County B', 'County C'],
    'State': ['State X', 'State Y', 'State Z'],
    'TotalPop': [100000, 150000, 80000],
    'Poverty': [8, 12, 10],  # Updated poverty rates
    'IncomePerCap': [50000, 40000, 45000]  # Updated income per capita
}
sample_df = pd.DataFrame(sample_data)

# Concatenate sample data with existing county_info DataFrame
county_info = pd.concat([county_info, sample_df], ignore_index=True)

# Displaying the first few rows of County_info DataFrame
print(county_info.head())

# Answer Questions: Most and least populous counties in the USA
sorted_county_info = county_info.sort_values(by='TotalPop', ascending=False)

most_populous_county = sorted_county_info.iloc[0]
least_populous_county = sorted_county_info.iloc[-1]

print("\nMost populous county in the USA:")
print(most_populous_county[['Name', 'State', 'TotalPop']])

print("\nLeast populous county in the USA:")
print(least_populous_county[['Name', 'State', 'TotalPop']])
