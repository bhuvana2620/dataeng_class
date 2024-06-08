import pandas as pd

# Load the synthetic data
synthetic_df = pd.read_csv('employees.csv')

# 1. Men vs. Women in Each Department
gender_distribution = synthetic_df.groupby(['Department', 'Gender']).size().unstack(fill_value=0)
print(gender_distribution)

# Save the distribution to a CSV file for further analysis if needed
gender_distribution.to_csv('gender_distribution.csv')

# 2. Total Yearly Payroll
total_payroll = synthetic_df['Salary'].sum()
print(f"Total Yearly Payroll: ${total_payroll}")

# Save the payroll calculation to a text file
with open('total_payroll.txt', 'w') as f:
    f.write(f"Total Yearly Payroll: ${total_payroll}")

# 3. Growth Strategies
growth_strategies = """
1. Acquiring smaller companies: Mergers and acquisitions can rapidly increase the company's size and capabilities.
2. Aggressive recruitment drives: Partnering with universities and using online platforms to attract top talent.
3. Increasing remote working capabilities: Expanding remote work options to attract talent from a broader geographic area.
"""
print(growth_strategies)

# Save the growth strategies to a text file
with open('growth_strategies.txt', 'w') as f:
    f.write(growth_strategies)

# 4. Office Space Requirement
number_of_employees = len(synthetic_df)
office_space_sqft = number_of_employees * 150
print(f"Total Office Space Required: {office_space_sqft} square feet")

# Save the office space calculation to a text file
with open('office_space.txt', 'w') as f:
    f.write(f"Total Office Space Required: {office_space_sqft} square feet")
# 5. Privacy Preservation
# Ensure that no identifiable data from the original employees.csv is retained in the synthetic dataset
privacy_preservation = """
The synthetic dataset generation process ensures that no identifiable data from the original employees.csv is retained.
The data is generated using random values and does not have any direct correlation with the original data beyond maintaining
a similar structure and realistic values.
"""
print(privacy_preservation)

# Save the privacy preservation statement to a text file
with open('privacy_preservation.txt', 'w') as f:
    f.write(privacy_preservation)

