import pandas as pd

# Load the CSV file
main_dir = "C:/Users/sarae/OneDrive - University of Calgary/Documents/Programming/Websites/CEarly Campaign/"
file_path = main_dir + "contacts_merged.csv"

df = pd.read_csv(file_path)

# Filter rows where 'ward' is 1
filtered_df = df[df['ward'] == '1']

# Helper function to extract the first comma-separated value
def extract_first_name(value):
    return value.split(',')[0].strip() if pd.notna(value) else ''

# Extract and format the names and email addresses
formatted_emails = []
for _, row in filtered_df.iterrows():
    first_name = extract_first_name(row['first name'])
    last_name = extract_first_name(row['last name'])
    email = row['email address']
    
    if pd.notna(email):
        formatted_emails.append(f"{first_name} {last_name} <{email}>; ")

# Combine the formatted strings
email_list = ''.join(formatted_emails)

# Print or save the result
print(email_list)
