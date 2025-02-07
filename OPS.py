import pandas as pd

def process_demat_data(onlineops_file, mis_file, output_eia_file, output_matched_file):
    """
    Processes the DEMAT account data by filtering invalid numbers,
    removing unnecessary records, and merging with the MIS data.

    Parameters:
        onlineops_file (str): Path to the Online OPS CSV file.
        mis_file (str): Path to the MIS CSV file.
        output_eia_file (str): Path to save the filtered EIA data.
        output_matched_file (str): Path to save the matched data.
    """

    # Read input CSV files
    df = pd.read_csv(onlineops_file, dtype=str)  # Read as strings to preserve leading zeros
    df_mis = pd.read_csv(mis_file, dtype=str)

    # Strip spaces and ensure clean column data
    df['DEMAT ACCOUNT NO'] = df['DEMAT ACCOUNT NO'].str.strip()

    # Remove rows where DEMAT ACCOUNT NO is the same as APPLICATION NO
    df = df[df['DEMAT ACCOUNT NO'].astype(str) != df['APPLICATION NO'].astype(str)]

    # Filter out valid 13-digit DEMAT numbers
    df_exact_13 = df[~df['DEMAT ACCOUNT NO'].apply(lambda x: not (pd.notna(x) and x.isdigit() and len(x) == 13))]
    df_start1to5 = df_exact_13[df_exact_13['DEMAT ACCOUNT NO'].str.startswith(('1', '2', '3', '4', '5'))]

    # Regex pattern to exclude invalid DEMAT numbers
    pattern1 = r'^(?!.*(\d)\1{12}$)(?!0123456789012)(?!9876543210987)(?!1234567890123)(?!9876543210987)(?!1111111111111)(?!9999999999999)(?!1000000000000)(?!1111111000000)(?!1111110000001)\d{13}$'

    # Apply regex to filter valid and invalid DEMAT numbers
    valid_data_pattern1 = df_start1to5[df_start1to5['DEMAT ACCOUNT NO'].str.match(pattern1, na=False)]
    df_pop_501 = valid_data_pattern1[~valid_data_pattern1['DEMAT ACCOUNT NO'].str.startswith('501')]

    # Apply additional conditions to filter valid DEMAT numbers
    df_valid_data3 = df_pop_501[
        (~df_pop_501['DEMAT ACCOUNT NO'].str.startswith(('1', '2'))) |
        (df_pop_501['DEMAT ACCOUNT NO'].str.startswith('1') & df_pop_501['DEMAT ACCOUNT NO'].str[:5].eq('10000')) |
        (df_pop_501['DEMAT ACCOUNT NO'].str.startswith('2') & df_pop_501['DEMAT ACCOUNT NO'].str[:5].eq('20000'))
    ]

    # Save filtered EIA numbers to CSV
    df_valid_data3.to_csv(output_eia_file, index=False)

    # Convert 'POLICY NO' and 'Contract No' to string and strip spaces
    df_valid_data3['POLICY NO'] = df_valid_data3['POLICY NO'].astype(str).str.strip()
    df_mis['Contract No'] = df_mis['Contract No'].astype(str).str.strip()

    # Merge with MIS data on POLICY NO and Contract No
    merged_df = df_valid_data3.merge(df_mis, left_on='POLICY NO', right_on='Contract No', how='left', indicator=True)

    # Keep only matched records
    matched_data = merged_df[merged_df['_merge'] == 'both'].drop(columns=['_merge', 'Contract No'])

    # Save matched data to CSV
    matched_data.to_csv(output_matched_file, index=False)

    print("Processing complete!")
    print(f"Filtered EIA data saved to: {output_eia_file}")
    print(f"Matched data saved to: {output_matched_file}")

# Example usage:
process_demat_data("onlineops1.csv", "MIScsv.csv", "EIA_No_Found_Online_OPS.csv", "online_ops_data_output.csv")
