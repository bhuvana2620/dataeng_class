import pandas as pd
from faker import Faker
import random

fake = Faker()

# Number of employees in the expanded company
num_employees = 10000

# Define departments and their percentages
departments = {
    'Legal': 0.05, 'Marketing': 0.10, 'Administrative': 0.10,
    'Operations': 0.20, 'Sales': 0.10, 'Finance': 0.05,
    'IT': 0.10, 'Product': 0.20, 'Human Resource': 0.10
}

# Define non-USA citizens proportions
countries = {
    'India': 0.23, 'China': 0.17, 'Canada': 0.12,
    'South Korea': 0.07, 'Philippines': 0.05,
    'Taiwan': 0.04, 'Mexico': 0.02
}

# Generating the synthetic employee data
synthetic_data = []

def generate_salary(department):
    if department == 'Marketing':
        return random.randint(67407, 90594)
    else:
        return random.randint(50000, 150000)

for _ in range(num_employees):
    department = random.choices(list(departments.keys()), list(departments.values()))[0]
    is_foreign = random.random() < 0.40
    country = random.choices(list(countries.keys()), list(countries.values()))[0] if is_foreign else 'USA'
    languages = random.choices([0, 1, 2], [0.5, 0.3, 0.2])
    language_list = [fake.language_name() for _ in range(languages[0])]
    gender = random.choice(['M', 'F'])
        
    employee = {
        'First Name': fake.first_name(),
        'Last Name': fake.last_name(),
        'Email': fake.email(),
        'Phone': fake.phone_number(),
        'Gender': gender,
        'Age': random.randint(22, 65),
        'Job Title': fake.job(),
        'Years of Experience': random.randint(0, 40),
        'Salary': generate_salary(department),
        'Department': department,
        'Country': country,
        'Languages': ', '.join(language_list)
    }
    
    synthetic_data.append(employee)

synthetic_df = pd.DataFrame(synthetic_data)
synthetic_df.to_csv('synthetic_employees.csv', index=False)

