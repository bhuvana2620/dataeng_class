import pandas as pd
import os

# Load the synthetic data
synthetic_df = pd.read_csv('synthetic_employees.csv')

# Define the weights for sampling
def age_bias(age):
    return 3 if 40 <= age <= 49 else 1

synthetic_df['Weight'] = synthetic_df['Age'].apply(age_bias)

# Produce a 20 element sample with the defined weights
biased_sample = synthetic_df.sample(n=20, weights=synthetic_df['Weight'], random_state=1)

# Drop the weights column for the final sample
biased_sample = biased_sample.drop(columns=['Weight'])

# Save the sample to a CSV file
biased_sample.to_csv('biased_sample.csv', index=False)

# Print the path to the CSV file
file_path = os.path.abspath('biased_sample.csv')
print(f"The path to the biased_sample.csv file is: {file_path}")