import pandas as pd
import json
import csv

# Load CSV file
input_file = 'reddit_aec_data.csv'
output_file = 'reddit_data.json'

# Read CSV with proper encoding and quoting to handle special characters
df = pd.read_csv(input_file, encoding='utf-8', quoting=csv.QUOTE_ALL)

# Convert DataFrame to a list of dictionaries
data = df.to_dict(orient='records')

# Write to JSON file
with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=4)

print(f"Successfully converted {input_file} to {output_file}")