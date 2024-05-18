import pandas as pd

# Update file paths to your local files
acs_data_path = "/home/bhu/acs2017_census_tract_data.csv"
covid_data_path = "/home/bhu/COVID_county_data.csv"

# Load ACS data
acs_data = pd.read_csv(acs_data_path)

# Filter and select the required columns from the ACS data
required_columns = ['County', 'State', 'TotalPop', 'IncomePerCap', 'Poverty']
acs_data_filtered = acs_data[required_columns].copy()

# Aggregate data by county and state
aggregated_data = acs_data_filtered.groupby(['County', 'State']).agg({
    'TotalPop': 'sum',
    'IncomePerCap': 'mean',
    'Poverty': 'mean'
})

# Reset the index of the aggregated DataFrame
County_Info = aggregated_data.reset_index()

# Rename columns for consistency
County_Info.rename(columns={'County': 'county', 'TotalPop': 'Population', 'IncomePerCap': 'PerCapitaIncome'}, inplace=True)

# Assign unique ID to each county
County_Info['ID'] = County_Info.index + 1

# Load COVID data
covid_data = pd.read_csv(covid_data_path)

# Create unique ID for each county in the COVID data
covid_data['ID'] = covid_data['county'] + '_' + covid_data['state']
covid_data['ID'] = pd.factorize(covid_data['ID'])[0] + 1

# Convert date column to datetime format and extract month
covid_data['date'] = pd.to_datetime(covid_data['date'], format='%Y-%m-%d')
covid_data['month'] = covid_data['date'].dt.to_period('M')

# Aggregate COVID data by county and month
covid_data_monthly = covid_data.groupby(['ID', 'month']).agg({
    'cases': 'sum',
    'deaths': 'sum',
    'county': 'first'
}).reset_index()

# Assign new IDs to the monthly COVID data
covid_data_monthly['ID'] = range(1, len(covid_data_monthly) + 1)
covid_data_monthly = covid_data_monthly[['ID', 'month', 'cases', 'deaths', 'county']].copy()
covid_data_monthly.columns = ['ID', 'Month', 'cases', 'deaths', 'county']

# Summarize total cases and deaths for each county
covid_summary = covid_data_monthly.groupby("ID").agg({"cases": "sum", "deaths": "sum"}).reset_index()
covid_summary.rename(columns={"cases": "TotalCases", "deaths": "TotalDeaths"}, inplace=True)

# Merge COVID summary with County_Info
covid_summary = pd.merge(covid_summary, County_Info, how="left", on="ID")
covid_summary["TotalCasesPer100K"] = round((covid_summary["TotalCases"]) / (covid_summary["Population"] / 100000), 2)
covid_summary["TotalDeathsPer100K"] = round((covid_summary["TotalDeaths"]) / (covid_summary["Population"] / 100000), 2)

# Filter for Oregon counties
oregon_county = covid_summary[covid_summary['State'] == 'Oregon']

# Compute correlations for Oregon counties
analysis_1a = oregon_county['TotalCases'].corr(oregon_county['Poverty'])
analysis_1b = oregon_county['TotalDeaths'].corr(oregon_county['Poverty'])
analysis_1c = oregon_county['TotalCases'].corr(oregon_county['PerCapitaIncome'])
analysis_1d = oregon_county['TotalDeaths'].corr(oregon_county['PerCapitaIncome'])

# Compute correlations for all USA counties
r_cases_poverty_usa = covid_summary['TotalCases'].corr(covid_summary['Poverty'])
r_deaths_poverty_usa = covid_summary['TotalDeaths'].corr(covid_summary['Poverty'])
r_cases_income_usa = covid_summary['TotalCases'].corr(covid_summary['PerCapitaIncome'])
r_deaths_income_usa = covid_summary['TotalDeaths'].corr(covid_summary['PerCapitaIncome'])
r_cases_population_usa = covid_summary['TotalCases'].corr(covid_summary['Population'])

# Print the results
print(f"For Oregon Counties only: correlation between % poverty and COVID cases: {analysis_1a:.2f}")
print(f"For Oregon Counties only: correlation between % poverty and COVID deaths: {analysis_1b:.2f}")
print(f"For Oregon Counties only: correlation between PerCapitaIncome and COVID cases: {analysis_1c:.2f}")
print(f"For Oregon Counties only: correlation between PerCapitaIncome and COVID deaths: {analysis_1d:.2f}")
print(f"For all USA counties: correlation between % poverty and COVID cases: {r_cases_poverty_usa:.2f}")
print(f"For all USA counties: correlation between % poverty and COVID deaths: {r_deaths_poverty_usa:.2f}")
print(f"For all USA counties: correlation between PerCapitaIncome and COVID cases: {r_cases_income_usa:.2f}")
print(f"For all USA counties: correlation between PerCapitaIncome and COVID deaths: {r_deaths_income_usa:.2f}")
print(f"For all USA counties: correlation between Population and COVID cases: {r_cases_population_usa:.2f}")
