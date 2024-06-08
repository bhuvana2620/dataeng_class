import pandas as pd
from ydata_profiling import ProfileReport

# Load the original employees data
original_df = pd.read_csv('employees.csv')

# Generate profile report for the original data
profile_original = ProfileReport(original_df, title="Original Data Profile")
profile_original.to_file("original_data_profile.html")

# Load the perturbed data
perturbed_df = pd.read_csv('perturbed_employees.csv')

# Generate profile report for the perturbed data
profile_perturbed = ProfileReport(perturbed_df, title="Perturbed Data Profile")
profile_perturbed.to_file("perturbed_data_profile.html")