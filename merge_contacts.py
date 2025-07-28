import pandas as pd
import re

# merge old google sheet contacts w new outlook contacts 
# double check - haven't run since adding new addvanced voter list 

main_dir = "C:/Users/sarae/OneDrive - University of Calgary/Documents/Programming/Websites/CEarly Campaign/"
main_dir = "/home/saraearly/OneDrive/Documents/CEarly Campaign/"
# Load the CSV files
contacts_old = pd.read_csv(main_dir + "contacts_Dec2024.csv")
contacts_new = pd.read_csv(main_dir + "contacts.csv")

def normalize_columns(df):
    df.columns = (
        df.columns.str.lower()
        .str.replace('e-mail', 'email', regex=False)
        .str.replace(' ', '_')
        .str.replace("'", "")
    )
    return df

contacts_old = normalize_columns(contacts_old)
contacts_new = normalize_columns(contacts_new)



# Define the rules for updating and merging
column_mapping = {
    "email address": "e-mail address",
    "phone number": "mobile phone",
    "street address": "business street",
    "city": "business city",
    "province": "business state",
    "postal code": "business postal code",
}

def determine_ward(row):
    # Safely check if the 'company' and 'job title' are strings before calling .lower()
    company = str(row.get("company", "")).lower() if isinstance(row.get("company", ""), str) else ""
    job_title = str(row.get("job title", "")).lower() if isinstance(row.get("job title", ""), str) else ""
    
    for field in [company, job_title]:
        match = re.search(r"ward\s+(\d+)", field)
        if match:
            return match.group(1)
    return None

# Map new columns to old columns
for old_col, new_col in column_mapping.items():
    contacts_new[old_col] = contacts_new[new_col]

# Determine the ward value for new contacts
contacts_new["ward"] = contacts_new.apply(
    lambda row: determine_ward(row) if pd.isna(row.get("ward", None)) else row["ward"], axis=1
)

ward_pattern = r"\bward\s+\d+\b"
contacts_new['company'] = contacts_new['company'].fillna('').astype(str)
contacts_new.loc[contacts_new['company'].str.lower().str.match(ward_pattern), 'company'] = ''

# Concatenate the 'department' and 'job title' column text to the 'company' column text, separated by commas
def concatenate_company_info(row):
    parts = []
    if pd.notna(row['company']) and row['company'] != "":
        parts.append(row['company'])
    if pd.notna(row['department']) and row['department'] != "":
        parts.append(row['department'])
    if pd.notna(row['job title']) and row['job title'] != "":
        parts.append(row['job title'])
    return ', '.join(parts).strip()

contacts_new['company'] = contacts_new.apply(concatenate_company_info, axis=1)

contacts_new = contacts_new.reindex(columns=contacts_old.columns, fill_value="")

###############################################################################################################

# Concatenate the old and new contacts
final_merged_df = pd.concat([contacts_old, contacts_new], ignore_index=True)

# Remove rows where 'email address' contains '/o=ExchangeLabs/'
print(final_merged_df.columns)
final_merged_df = final_merged_df[~final_merged_df['email address'].str.contains('/o=ExchangeLabs/', na=False)]
final_merged_df = final_merged_df[~final_merged_df['email address'].str.contains('/o=Town.Caledon', na=False)]
final_merged_df = final_merged_df[~final_merged_df['email address'].str.contains('O=Town.Caledon.on.ca', na=False)]

# Separate rows with empty email addresses
empty_email_rows = final_merged_df[final_merged_df['email address'].isna() | (final_merged_df['email address'] == '')]
non_empty_email_rows = final_merged_df[final_merged_df['email address'] != '']

# Combine different values for the same email address with a comma
def combine_values(series):
    unique_values = series.dropna().unique()
    return ', '.join(map(str, unique_values))

# Group by email address and combine values
final_merged_df_unique = non_empty_email_rows.groupby('email address').agg(combine_values).reset_index()

###############################################################################################################

# Fill missing first name values by searching in contacts_new
contacts_new = pd.read_csv(main_dir + "contacts.csv")

def fill_first_name(row):
    if not row['first name']:
        email = row['email address']
        matching_rows = contacts_new.apply(lambda x: any(email in str(cell) for cell in x.values), axis=1)
        if matching_rows.any():
            non_empty_first_names = contacts_new.loc[matching_rows & contacts_new['First Name'].notna(), 'First Name']
            if not non_empty_first_names.empty:
                return non_empty_first_names.values[0]
    return row['first name']

final_merged_df_unique['first name'] = final_merged_df_unique.apply(fill_first_name, axis=1)

# Add rows with empty email addresses to final_merged_df_unique
final_merged_df_unique = pd.concat([final_merged_df_unique, empty_email_rows], ignore_index=True)

# Reorder columns to have first name, last name, email address as the first three columns
final_merged_df_unique = final_merged_df_unique[['first name', 'last name', 'email address'] + [col for col in final_merged_df_unique.columns if col not in ['first name', 'last name', 'email address']]]

def clean_ward_value(value):
    if isinstance(value, str) and ',' in value:
        int_values = [int(float(v)) for v in value.split(',') if v.strip()]
        unique_values = list(set(int_values))
        if len(unique_values) == 1:
            return unique_values[0]
        return ', '.join(map(str, unique_values))
    elif pd.notna(value) and value != '':
        return str(int(float(value)))
    return value

final_merged_df_unique['ward'] = final_merged_df_unique['ward'].apply(clean_ward_value)

# Save the merged result to a new CSV
final_merged_df_unique.to_csv(main_dir + "contacts_merged.csv", index=False)

print("Contacts merged and saved successfully.")