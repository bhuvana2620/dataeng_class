import pandas as pd
import numpy as np
import datetime as dt

# Load datasets
c_df = pd.read_csv('COVID_county_data.csv')
a_df = pd.read_csv('acs2017_census_tract_data.csv')

# Select relevant columns from ACS data
cinfo_df = a_df[['County', 'State', 'TotalPop', 'Poverty', 'IncomePerCap']]

# Group by County and State and aggregate population, poverty, and income data
grp = cinfo_df.groupby(['County', 'State']).groups
county_df = pd.DataFrame(columns=['Id', 'County', 'State', 'TotalPop', 'Poverty', 'IncomePerCap'])
counter = 0
for g in grp:
    total_pop = 0
    total_poverty = 0
    total_income = 0
    for indx in grp[g]:
        row = cinfo_df.loc[indx]
        if np.isnan(row['Poverty']) or np.isnan(row['IncomePerCap']):
            continue
        population = row['TotalPop']
        total_pop += population
        total_poverty += (row['Poverty'] if row['Poverty'] else 0) * population / 100
        total_income += (row['IncomePerCap'] if row['IncomePerCap'] else 0) * population
    current = cinfo_df.loc[grp[g][0]]
    average_poverty = total_poverty * 100 / total_pop
    average_percapita = total_income / total_pop
    county_df.loc[counter] = [counter + 1, current['County'].replace(' County', ''), current['State'], total_pop, average_poverty, average_percapita]
    counter += 1

# Format the COVID data
c_df['ym'] = c_df['date'].apply(lambda x: dt.datetime.strptime(x, '%Y-%m-%d').strftime('%Y-%m'))

# Group COVID data by county, state, and month
grp = c_df.groupby(['county', 'state', 'ym']).groups

# Create a DataFrame to store monthly COVID data aggregated by county and state
covid_df = pd.DataFrame(columns=['Date', 'County', 'State', 'Cases', 'Deaths'])
counter = 0
for g in grp:
    total_cases = 0
    total_deaths = 0
    for indx in grp[g]:
        row = c_df.loc[indx]
        total_cases += row['cases']
        total_deaths += row['deaths']
    covid_df.loc[counter] = [g[2], g[0], g[1], total_cases, total_deaths]
    counter += 1

# Merge county data with COVID data
temp_df = pd.merge(county_df, covid_df, on=['County', 'State'], how='outer')

# Group by county and state to aggregate COVID data
grp = temp_df.groupby(['County', 'State']).groups
covid_summary_df = pd.DataFrame(columns=['County', 'State', 'Population', 'Poverty', 'IncomePerCap', 'TotalCases', 'TotalDeaths', 'TotalCasesPer100K', 'TotalDeathsPer100K'])

counter = 0
for g in grp:
    total_cases = 0
    total_deaths = 0
    for indx in grp[g]:
        row = temp_df.loc[indx]
        total_cases += row['Cases']
        total_deaths += row['Deaths']
    total_population = temp_df.loc[grp[g][0]]['TotalPop']
    poverty = temp_df.loc[grp[g][0]]['Poverty']
    income_per_capita = temp_df.loc[grp[g][0]]['IncomePerCap']
    cases_per_100k = total_cases / (total_population / 100000)
    deaths_per_100k = total_deaths / (total_population / 100000)
    covid_summary_df.loc[counter] = [g[0], g[1], total_population, poverty, income_per_capita, total_cases, total_deaths, cases_per_100k, deaths_per_100k]
    counter += 1

# Extract information for specific counties
washington_county_oregon = covid_summary_df[(covid_summary_df['County'] == 'Washington') & (covid_summary_df['State'] == 'Oregon')]
malheur_county_oregon = covid_summary_df[(covid_summary_df['County'] == 'Malheur') & (covid_summary_df['State'] == 'Oregon')]
loudoun_county_virginia = covid_summary_df[(covid_summary_df['County'] == 'Loudoun') & (covid_summary_df['State'] == 'Virginia')]
harlan_county_kentucky = covid_summary_df[(covid_summary_df['County'] == 'Harlan') & (covid_summary_df['State'] == 'Kentucky')]

# Display results
print("Washington County, Oregon:")
print(washington_county_oregon[['County', 'Poverty', 'TotalCasesPer100K']])

print("\nMalheur County, Oregon:")
print(malheur_county_oregon[['County', 'Poverty', 'TotalCasesPer100K']])

print("\nLoudoun County, Virginia:")
print(loudoun_county_virginia[['County', 'Poverty', 'TotalCasesPer100K']])

print("\nHarlan County, Kentucky:")
print(harlan_county_kentucky[['County', 'Poverty', 'TotalCasesPer100K']])
