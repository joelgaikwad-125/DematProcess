import os
import pandas as pd

# File paths (update with actual file paths)
files = [
    r"C:\Users\ADMIN\Desktop\GIT\Output\EIA_No_Found_CCD.csv",
    r"C:\Users\ADMIN\Desktop\GIT\Output\EIA_No_Found_OPS.csv",
    r"C:\Users\ADMIN\Desktop\GIT\Output\EIA_No_Found_TEBT.csv"
]

# Step 1: Consolidate files (Skip missing files)
dataframes = []
for file in files:
    if os.path.exists(file):
        dataframes.append(pd.read_csv(file))
    else:
        print(f"Warning: File not found - {file}")

# Only concatenate if files were found
if dataframes:
    consolidated_df = pd.concat(dataframes, ignore_index=True)
    consolidated_df = consolidated_df.sort_values(by='POLICY_NO').drop_duplicates(subset='POLICY_NO')
else:
    print("No valid input files found. Exiting...")
    exit()

# Load existing MIS file
mis_file_path = r"C:\Users\ADMIN\Desktop\GIT\Input\MIS.xlsx"

if os.path.exists(mis_file_path):
    df_mis = pd.read_excel(mis_file_path)
else:
    df_mis = pd.DataFrame()  # Create empty dataframe if MIS file does not exist

# Ensure unique column names
df_mis = df_mis.loc[:, ~df_mis.columns.duplicated()]
consolidated_df = consolidated_df.loc[:, ~consolidated_df.columns.duplicated()]

# Ensure consistent column naming (handling case sensitivity issues)
if "Policy number" in df_mis.columns:
    df_mis.rename(columns={"Policy number": "Contract No"}, inplace=True)
if "POLICY_NO" in consolidated_df.columns:
    consolidated_df.rename(columns={"POLICY_NO": "Contract No"}, inplace=True)

# Append new data to existing MIS file
df_combined = pd.concat([df_mis, consolidated_df], ignore_index=True).dropna(how='all')

# Save the updated MIS file
df_combined.to_excel(mis_file_path, index=False)
print(f"New data appended to '{mis_file_path}' successfully.")

# Step 2: Load Demat Scan Report
demat_scan_file_path = r"C:\Users\ADMIN\Desktop\GIT\Input\Demat Scan.xlsx"

if os.path.exists(demat_scan_file_path):
    demat_scan_df = pd.read_excel(demat_scan_file_path)
    
    # Standardize column names
    if "Policy Number" in demat_scan_df.columns:
        demat_scan_df.rename(columns={"Policy Number": "Contract No"}, inplace=True)
    elif "Policy number" in demat_scan_df.columns:
        demat_scan_df.rename(columns={"Policy number": "Contract No"}, inplace=True)

    # Ensure 'Contract No' exists before proceeding
    if "Contract No" not in demat_scan_df.columns:
        print("Error: 'Contract No' column not found in Demat Scan file.")
        exit()

    # Map Demat Scan data to MIS format
    demat_data = pd.DataFrame({
        "Contract No": demat_scan_df["Contract No"],
        "Source": demat_scan_df["Source"],
        "IR": ["NSDL" for _ in range(len(demat_scan_df))],  # Static "NSDL" for all rows
    })

    # Append Demat Scan data
    combined_df = pd.concat([df_combined, demat_data], ignore_index=True).drop_duplicates(subset="Contract No")

    # Save updated MIS file
    combined_df.to_excel(mis_file_path, index=False)
    print(f"Demat Scan data appended to MIS report successfully: '{mis_file_path}'")
else:
    print(f"Warning: Demat Scan file not found - {demat_scan_file_path}")

# Step 3: Generate CCH file (for missing values)
cch_df = combined_df[
    combined_df["Type"].isnull() | combined_df["Prod Code"].isnull() | combined_df["Agency Code"].isnull()
][["Contract No", "Prod Code", "Type", "Agency Code"]]

cch_file_path = r"C:\Users\ADMIN\Desktop\GIT\Output\CCH_File_To_be_Checked.xlsx"
cch_df.to_excel(cch_file_path, index=False)
print(f"CCH file generated successfully: '{cch_file_path}'")

# Step 4: Filter for Conversion cases
df_conv = combined_df[~combined_df['Contract No'].astype(str).str.startswith('9')]

# Exclude specific Remarks
remarks_to_exclude = [
    "Policy Credited in eIA A/C",
    "Policy Credited in eIA A/C & Policy Despatched in Physical Format",
    "Annuity Cases out of Scope",
    "C2P Health Case - Not to Process",
    "eIA Converted from SHCIL to CAMS",
    'eIA Opened & Policy Despatched in Physical Format',
    "Health Policy out of Scope",
    "HUF Case - Not to Process",
    "J & K Case out of Scope",
    "Online (OPS) Cases - Not to Process",
    "Policy Dispatched in Physical format due to non compliance of FR",
    "Policy Dispatched in Physical format due to non compliance of FR in 10 days",
    "SHCIL - Not to Process"
]

df_conv = df_conv[~df_conv['Remarks'].isin(remarks_to_exclude)]

# Save to Conv.txt
conv_file_path = r"C:\Users\ADMIN\Desktop\GIT\Output\Conv.txt"
with open(conv_file_path, 'w') as f:
    for contract_no in df_conv['Contract No']:
        f.write(f"{contract_no}\n")

print(f"Filtered policy numbers written to '{conv_file_path}' successfully.")
