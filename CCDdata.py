
import pandas as pd
from datetime import datetime
import time

# Load the data
data_demat_Details = pd.read_excel("DMAT_DETAILS_20241107_aniket.xlsx")
data_demat_Details2=pd.read_excel("MIS.xlsx")
#print(data_demat_Details2)
#print(data_demat_Details)

# Filter rows where DEMAT_FLG is 'Y'
filtered_data = data_demat_Details[data_demat_Details['DEMAT_FLG'] == 'Y']

# Ensure 'POLICY_NO' is a string and filter out NaN/empty values
filtered_data1 = filtered_data[
    filtered_data['POLICY_NO'].notna() &
    (filtered_data['POLICY_NO'].astype(str).str.strip() != '')
]

# Strip leading/trailing spaces from 'DEMAT_ACCOUNT_NO' and 'POS_APPLICATION_NO'
data_demat_Details['DEMAT_ACCOUNT_NO'] = data_demat_Details['DEMAT_ACCOUNT_NO'].astype(str).str.strip()
data_demat_Details['POS_APPLICATION_NO'] = data_demat_Details['POS_APPLICATION_NO'].astype(str).str.strip()

# Remove rows where 'DEMAT_ACCOUNT_NO' is the same as 'POS_APPLICATION_NO'
data_demat_Details1 = data_demat_Details[data_demat_Details['DEMAT_ACCOUNT_NO'] != data_demat_Details['POS_APPLICATION_NO']]

# Filter rows where 'DEMAT_ACCOUNT_NO' is a 13-digit number
df_exact_13 = data_demat_Details1[~data_demat_Details1['DEMAT_ACCOUNT_NO'].apply(lambda x: not (pd.notna(x) and x.isdigit() and len(x) == 13))]

# Filter rows where 'DEMAT_ACCOUNT_NO' starts with 1, 2, 3, 4, or 5
df_start1to5 = df_exact_13[df_exact_13['DEMAT_ACCOUNT_NO'].str.startswith(('1', '2', '3', '4', '5'))]

# Define the regex pattern to filter valid DEMAT numbers
pattern1 = r'^(?!.*(\d)\1{12}$)(?!0123456789012)(?!9876543210987)(?!1234567890123)(?!9876543210987)(?!1111111111111)(?!9999999999999)(?!1000000000000)(?!1111111000000)(?!1111110000001)\d{13}$'

# Apply regex to filter valid DEMAT numbers
valid_data_pattern1 = df_start1to5[df_start1to5['DEMAT_ACCOUNT_NO'].str.match(pattern1, na=False)]

# Remove rows where 'DEMAT_ACCOUNT_NO' starts with '501'
df_pop_501 = valid_data_pattern1[~valid_data_pattern1['DEMAT_ACCOUNT_NO'].str.startswith('501')]

# Further filter based on certain conditions
df_valid_data3 = df_pop_501[
    (~df_pop_501['DEMAT_ACCOUNT_NO'].str.startswith(('1', '2'))) |
    (df_pop_501['DEMAT_ACCOUNT_NO'].str.startswith('1') & df_pop_501['DEMAT_ACCOUNT_NO'].str[:5].eq('10000')) |
    (df_pop_501['DEMAT_ACCOUNT_NO'].str.startswith('2') & df_pop_501['DEMAT_ACCOUNT_NO'].str[:5].eq('20000'))
]

#add SOURCE column
df_valid_data3['SOURCE']='CCD Data'

df_valid_data3['IR']=''

# Rename 'DEMAT ACCOUNT NO' to 'E_IA_NO'
df_valid_data3.rename(columns={'DEMAT_ACCOUNT_NO': 'E_IA_NO'}, inplace=True)

df_valid_data3['TYPE']=''

# Add 'Date' column with the current date
df_valid_data3['Date'] = pd.to_datetime('today').strftime('%Y-%m-%d')

# Save filtered data to CSV
df_valid_data3.to_csv("EIA_No_Found_CCD.csv", index=False)

# Print success message
print("Successfully created",df_valid_data3)


