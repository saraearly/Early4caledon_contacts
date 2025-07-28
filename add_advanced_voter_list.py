import pandas as pd
import re

main_dir = "C:/Users/sarae/OneDrive - University of Calgary/Documents/CEarly Campaign/"
main_dir = "/home/saraearly/OneDrive/Documents/CEarly Campaign/"
csv_path = main_dir + 'contacts_merged.csv'  # Update with your CSV file path
csv_df = pd.read_csv(csv_path)

def process_name_cell(value):
    if isinstance(value, str):  
        value = re.sub(r'\s*AND,\s*', '', value).strip()
        words = value.split(', ')
        word_counts = {word: words.count(word) for word in words}
        repeated_word = [word for word, count in word_counts.items() if count > 1]
        if repeated_word:
            return repeated_word[0].upper()
        else:
            return ', '.join(sorted(set(words), key=str.lower)).upper()
    
    return value 

csv_df['first name'] = csv_df['first name'].apply(process_name_cell)
csv_df['last name'] = csv_df['last name'].apply(process_name_cell)
csv_df.to_csv(main_dir + 'contacts_old.csv' , index=False)

csv_df['advanced voter'] = ''

######################################################################################################################
excel_df = pd.read_excel(main_dir + 'advanced_voter_list/Advanced Polls voters.xlsx')
print(len(excel_df))
unmatched_rows = excel_df.copy()

indices_to_drop = []

for index, row in csv_df.iterrows():
    first_name_csv = row['first name']
    last_name_csv = row['last name']
    
    match_row = excel_df[
        ((excel_df['First Name'].str.contains(str(first_name_csv), case=False, na=False)) | 
        (excel_df['First Name'].apply(lambda x: str(x).lower() in str(first_name_csv).lower() if pd.notna(x) else False))) &
        (excel_df['Last Name'].apply(lambda x: str(x).lower() in str(last_name_csv).lower() if pd.notna(x) else False))
    ]

    if not match_row.empty:
        street_no = int(match_row.iloc[0]['#'])
        street_name = match_row.iloc[0]['STREET']
        street_address = f"{street_no} {street_name}"
    
        # Update the 'Street Address' in the CSV
        csv_df.at[index, 'street address'] = street_address
            
        # Check if the 'HAVE SIGN ?' column contains 'SIGN'
        if pd.notna(match_row.iloc[0]['HAVE SIGN ?']) and 'SIGN' in str(match_row.iloc[0]['HAVE SIGN ?']):
            csv_df.at[index, '2022 sign'] = 'Yes'

        csv_df.at[index, 'advanced voter'] = 'Yes'

        # Collect indices of matched rows to drop
        indices_to_drop.append(match_row.index[0])

unmatched_rows = unmatched_rows.drop(indices_to_drop)

new_rows = unmatched_rows[['First Name', 'Last Name', '#', 'STREET', 'HAVE SIGN ?']].copy()

new_rows['#'] = pd.to_numeric(new_rows['#'], errors='coerce')  # Ensure numeric; invalid values become NaN
new_rows['#'] = new_rows['#'].fillna("").apply(lambda x: str(int(x)) if x != "" else "")  # Handle NaN and convert to int -> string
new_rows['street address'] = new_rows['#'] + ' ' + new_rows['STREET']

new_rows['2022 sign'] = new_rows['HAVE SIGN ?'].apply(lambda x: 'Yes' if isinstance(x, str) and 'SIGN' in x else '')
new_rows['first name'] = new_rows['First Name']
new_rows['last name'] = new_rows['Last Name']
new_rows['advanced voter'] = 'Yes'

csv_df = pd.concat([csv_df, new_rows[['first name', 'last name', 'street address', '2022 sign','advanced voter']]], ignore_index=True)

######################################################################################################################
excel_df = pd.read_excel(main_dir + 'advanced_voter_list/New Advanced Polls.xlsx')
print(len(excel_df))
unmatched_rows = excel_df.copy()

indices_to_drop = []

for index, row in csv_df.iterrows():
    first_name_csv = row['first name']
    last_name_csv = row['last name']
    
    match_row = excel_df[
        ((excel_df['First Name'].str.contains(str(first_name_csv), case=False, na=False)) | 
        (excel_df['First Name'].apply(lambda x: str(x).lower() in str(first_name_csv).lower() if pd.notna(x) else False))) &
        (excel_df['Last Name'].apply(lambda x: str(x).lower() in str(last_name_csv).lower() if pd.notna(x) else False))
    ]

    if not match_row.empty:
        street_no = int(match_row.iloc[0]['Street Number'])
        street_name = match_row.iloc[0]['Street Name/Type/Direction']
        street_address = f"{street_no} {street_name}"
    
        csv_df.at[index, 'street address'] = street_address
        csv_df.at[index, 'ward'] = int(match_row.iloc[0]['Ward'])

        csv_df.at[index, 'advanced voter'] = 'Yes'

        indices_to_drop.append(match_row.index[0])

unmatched_rows = unmatched_rows.drop(indices_to_drop)

new_rows = unmatched_rows[['First Name', 'Last Name', 'Street Number', 'Street Name/Type/Direction', 'Ward']].copy()


new_rows['Street Number'] = pd.to_numeric(new_rows['Street Number'], errors='coerce')  # Ensure numeric; invalid values become NaN
new_rows['Street Number'] = new_rows['Street Number'].fillna("").apply(lambda x: str(int(x)) if x != "" else "")  # Handle NaN and convert to int -> string
new_rows['street address'] = new_rows['Street Number'] + ' ' + new_rows['Street Name/Type/Direction']

new_rows['ward'] = new_rows['Ward']
new_rows['first name'] = new_rows['First Name']
new_rows['last name'] = new_rows['Last Name']
new_rows['advanced voter'] = 'Yes'

csv_df = pd.concat([csv_df, new_rows[['first name', 'last name', 'street address', 'ward','advanced voter']]], ignore_index=True)

######################################################################################################################
excel_df = pd.read_excel(main_dir + 'advanced_voter_list/Ward 1 List of Recorded Electors - October 14.xlsx')
print(len(excel_df))
ward_num = 1

unmatched_rows = excel_df.copy()

indices_to_drop = []

for index, row in csv_df.iterrows():
    first_name_csv = row['first name']
    last_name_csv = row['last name']
    
    match_row = excel_df[
        ((excel_df['First Name'].str.contains(str(first_name_csv), case=False, na=False)) | 
        (excel_df['First Name'].apply(lambda x: str(x).lower() in str(first_name_csv).lower() if pd.notna(x) else False))) &
        (excel_df['Last Name'].apply(lambda x: str(x).lower() in str(last_name_csv).lower() if pd.notna(x) else False))
    ]

    if not match_row.empty:
        if pd.notna(match_row.iloc[0]['Street No.']):
            street_no = int(match_row.iloc[0]['Street No.'])
        else:
            street_no = ''
        street_name = match_row.iloc[0]['Street Name']
        street_type = match_row.iloc[0]['Street Type']
        street_address = f"{street_no} {street_name} {street_type}"
    
        csv_df.at[index, 'street address'] = street_address
        csv_df.at[index, 'ward'] = ward_num

        csv_df.at[index, 'advanced voter'] = 'Yes'

        indices_to_drop.append(match_row.index[0])

unmatched_rows = unmatched_rows.drop(indices_to_drop)

new_rows = unmatched_rows[['First Name', 'Last Name', 'Street No.', 'Street Name', 'Street Type']].copy()

new_rows['Street No.'] = pd.to_numeric(new_rows['Street No.'], errors='coerce')  # Ensure numeric; invalid values become NaN
new_rows['Street No.'] = new_rows['Street No.'].fillna("").apply(lambda x: str(int(x)) if x != "" else "")  # Handle NaN and convert to int -> string
new_rows['street address'] = new_rows['Street No.'] + ' ' + new_rows['Street Name'] + ' ' + new_rows['Street Type']

new_rows['ward'] = ward_num
new_rows['first name'] = new_rows['First Name']
new_rows['last name'] = new_rows['Last Name']
new_rows['advanced voter'] = 'Yes'

csv_df = pd.concat([csv_df, new_rows[['first name', 'last name', 'street address', 'ward','advanced voter']]], ignore_index=True)

######################################################################################################################
excel_df = pd.read_excel(main_dir + 'advanced_voter_list/Ward 2 List of Recorded Electors - October 14.xlsx')
print(len(excel_df))
ward_num = 2

unmatched_rows = excel_df.copy()

indices_to_drop = []

for index, row in csv_df.iterrows():
    first_name_csv = row['first name']
    last_name_csv = row['last name']
    
    match_row = excel_df[
        ((excel_df['First Name'].str.contains(str(first_name_csv), case=False, na=False)) | 
        (excel_df['First Name'].apply(lambda x: str(x).lower() in str(first_name_csv).lower() if pd.notna(x) else False))) &
        (excel_df['Last Name'].apply(lambda x: str(x).lower() in str(last_name_csv).lower() if pd.notna(x) else False))
    ]

    if not match_row.empty:
        if pd.notna(match_row.iloc[0]['Street No.']):
            street_no = int(match_row.iloc[0]['Street No.'])
        else:
            street_no = ''
        street_name = match_row.iloc[0]['Street Name']
        street_type = match_row.iloc[0]['Street Type']
        street_address = f"{street_no} {street_name} {street_type}"
    
        csv_df.at[index, 'street address'] = street_address
        csv_df.at[index, 'ward'] = ward_num

        csv_df.at[index, 'advanced voter'] = 'Yes'

        indices_to_drop.append(match_row.index[0])

unmatched_rows = unmatched_rows.drop(indices_to_drop)

new_rows = unmatched_rows[['First Name', 'Last Name', 'Street No.', 'Street Name', 'Street Type']].copy()

new_rows['Street No.'] = pd.to_numeric(new_rows['Street No.'], errors='coerce')  # Ensure numeric; invalid values become NaN
new_rows['Street No.'] = new_rows['Street No.'].fillna("").apply(lambda x: str(int(x)) if x != "" else "")  # Handle NaN and convert to int -> string
new_rows['street address'] = new_rows['Street No.'] + ' ' + new_rows['Street Name'] + ' ' + new_rows['Street Type']

new_rows['ward'] = ward_num
new_rows['first name'] = new_rows['First Name']
new_rows['last name'] = new_rows['Last Name']
new_rows['advanced voter'] = 'Yes'

csv_df = pd.concat([csv_df, new_rows[['first name', 'last name', 'street address', 'ward','advanced voter']]], ignore_index=True)

######################################################################################################################
excel_df = pd.read_excel(main_dir + 'advanced_voter_list/Ward 3 List of Recorded Electors - October 14.xlsx')
print(len(excel_df))
ward_num = 3

unmatched_rows = excel_df.copy()

indices_to_drop = []

for index, row in csv_df.iterrows():
    first_name_csv = row['first name']
    last_name_csv = row['last name']
    
    match_row = excel_df[
        ((excel_df['First Name'].str.contains(str(first_name_csv), case=False, na=False)) | 
        (excel_df['First Name'].apply(lambda x: str(x).lower() in str(first_name_csv).lower() if pd.notna(x) else False))) &
        (excel_df['Last Name'].apply(lambda x: str(x).lower() in str(last_name_csv).lower() if pd.notna(x) else False))
    ]

    if not match_row.empty:
        if pd.notna(match_row.iloc[0]['Street No.']):
            street_no =int(match_row.iloc[0]['Street No.'])
        else:
            street_no = ''
        street_name = match_row.iloc[0]['Street Name']
        street_type = match_row.iloc[0]['Street Type']
        street_address = f"{street_no} {street_name} {street_type}"
    
        csv_df.at[index, 'street address'] = street_address
        csv_df.at[index, 'ward'] = ward_num

        csv_df.at[index, 'advanced voter'] = 'Yes'

        indices_to_drop.append(match_row.index[0])

unmatched_rows = unmatched_rows.drop(indices_to_drop)

new_rows = unmatched_rows[['First Name', 'Last Name', 'Street No.', 'Street Name', 'Street Type']].copy()

new_rows['Street No.'] = pd.to_numeric(new_rows['Street No.'], errors='coerce')  # Ensure numeric; invalid values become NaN
new_rows['Street No.'] = new_rows['Street No.'].fillna("").apply(lambda x: str(int(x)) if x != "" else "")  # Handle NaN and convert to int -> string
new_rows['street address'] = new_rows['Street No.'] + ' ' + new_rows['Street Name'] + ' ' + new_rows['Street Type']

new_rows['ward'] = ward_num
new_rows['first name'] = new_rows['First Name']
new_rows['last name'] = new_rows['Last Name']
new_rows['advanced voter'] = 'Yes'

csv_df = pd.concat([csv_df, new_rows[['first name', 'last name', 'street address', 'ward','advanced voter']]], ignore_index=True)

######################################################################################################################

csv_df.to_csv(main_dir + 'contacts_added_advanced_voters_test.csv', index=False)