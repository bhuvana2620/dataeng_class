import pandas as pd
from faker import Faker
import os

# Load the synthetic data
synthetic_df = pd.read_csv('synthetic_employees.csv')

# Initialize the Faker library
fake = Faker()

# Function to anonymize data
def anonymize_data(df):
    df['First Name'] = df['First Name'].apply(lambda _: fake.first_name())
    df['Last Name'] = df['Last Name'].apply(lambda _: fake.last_name())
    df['Email'] = df['Email'].apply(lambda _: fake.email())
    df['Phone'] = df['Phone'].apply(lambda _: fake.phone_number())
    return df

# Anonymize the data
anonymized_df = anonymize_data(synthetic_df)

# Save the anonymized data to a new CSV file
anonymized_df.to_csv('anonymized_employees.csv', index=False)

# Print the path to the anonymized CSV file
file_path = os.path.abspath('anonymized_employees.csv')
print(f"The path to the anonymized_employees.csv file is: {file_path}")