import pandas as pd
import numpy as np
import os

# Load the anonymized data
anonymized_df = pd.read_csv('anonymized_employees.csv')

# Define standard deviations for the Gaussian noise
age_std = 2
salary_std = 5000
experience_std = 1

# Add Gaussian noise
anonymized_df['Age'] = anonymized_df['Age'] + np.random.normal(0, age_std, anonymized_df.shape[0])
anonymized_df['Salary'] = anonymized_df['Salary'] + np.random.normal(0, salary_std, anonymized_df.shape[0])
anonymized_df['Years of Experience'] = anonymized_df['Years of Experience'] + np.random.normal(0, experience_std, anonymized_df.shape[0])

# Ensure the values are within realistic bounds
anonymized_df['Age'] = anonymized_df['Age'].clip(22, 65)
anonymized_df['Salary'] = anonymized_df['Salary'].clip(50000, 150000)
anonymized_df['Years of Experience'] = anonymized_df['Years of Experience'].clip(0, 40)

# Save the perturbed data to a new CSV file
anonymized_df.to_csv('perturbed_employees.csv', index=False)

# Print the path to the perturbed CSV file
file_path = os.path.abspath('perturbed_employees.csv')
print(f"The path to the perturbed_employees.csv file is: {file_path}")