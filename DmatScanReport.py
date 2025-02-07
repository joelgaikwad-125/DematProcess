import pandas as pd

# Function to read Excel files
def read_excel_files():
    CCD_OP_DF = pd.read_excel('CCD_Data_Output.xlsx')
    TEBT_OP_DF = pd.read_excel('TEBT_Data_Output.xlsx')
    OPS_OP_DF = pd.read_excel('Online_Ops_Data_Output.xlsx')
    NBEMAIL_DF = pd.read_excel('DummyNBEMail.xlsx')
    DematScan_DF = pd.read_excel('Demat_scan_20250101.xls')
    MIS_DF = pd.read_excel('MIS_s.xlsx')
    return CCD_OP_DF, TEBT_OP_DF, OPS_OP_DF, NBEMAIL_DF, DematScan_DF, MIS_DF

# Function to handle duplicates and prioritization
def handle_duplicates(NBEMAIL_DF, CCD_OP_DF, TEBT_OP_DF):
    NBEMAIL_combined = NBEMAIL_DF[['POLNO']].rename(columns={'POLNO': 'Policy Number'})
    NBEMAIL_combined['Source'] = 'NB EMAIL'

    CCD_combined = CCD_OP_DF[['POLICY_NO']].rename(columns={'POLICY_NO': 'Policy Number'})
    CCD_combined['Source'] = 'CCD Data'

    TEBT_combined = TEBT_OP_DF[['POLICY_NO']].rename(columns={'POLICY_NO': 'Policy Number'})
    TEBT_combined['Source'] = 'TEBT Data'

    CCD_filtered = CCD_combined[~CCD_combined['Policy Number'].isin(NBEMAIL_combined['Policy Number'])]
    TEBT_filtered = TEBT_combined[~TEBT_combined['Policy Number'].isin(NBEMAIL_combined['Policy Number'])]
    TEBT_filtered = TEBT_filtered[~TEBT_filtered['Policy Number'].isin(CCD_filtered['Policy Number'])]

    combined_df = pd.concat([NBEMAIL_combined, CCD_filtered, TEBT_filtered], ignore_index=True)
    return combined_df

# Function to append Demat Scan Report
def append_demat_scan_report(combined_df, DematScan_DF):
    DematScan_combined = DematScan_DF[['POLICY NO']].rename(columns={'POLICY NO': 'Policy Number'})
    DematScan_combined['Source'] = 'Demat Scan Report'

    filtered_combined_df = combined_df[~combined_df['Policy Number'].isin(DematScan_combined['Policy Number'])]
    final_combined_df = pd.concat([DematScan_combined, filtered_combined_df], ignore_index=True)
    return final_combined_df

# Function to append Online OPS Data
def append_online_ops_data(final_combined_df, OPS_OP_DF):
    Online_OPS_combined = OPS_OP_DF[['POLICY NO']].rename(columns={'POLICY NO': 'Policy Number'})
    Online_OPS_combined['Source'] = 'Online OPS'

    filtered_final_df = final_combined_df[~final_combined_df['Policy Number'].isin(Online_OPS_combined['Policy Number'])]
    final_output_df = pd.concat([filtered_final_df, Online_OPS_combined], ignore_index=True)
    return final_output_df

# Function to perform lookup with MIS File
def perform_lookup_mis(final_output_df, MIS_DF):
    MIS_DF.rename(columns={'Contract No': 'Policy No'}, inplace=True)
    unmatched_cases_df = final_output_df[~final_output_df['Policy Number'].isin(MIS_DF['Policy No'])]
    return unmatched_cases_df

# Function to remove duplicates and create the final Excel output
def create_demat_scan_output(final_output_df, unmatched_cases_df):
    final_unique_df = final_output_df.drop_duplicates(subset=['Policy Number'], keep='first')
    filtered_unmatched_df = unmatched_cases_df.drop_duplicates(subset=['Policy Number'], keep='first')

    final_demat_scan_df = final_unique_df[['Policy Number', 'Source']].copy()
    final_demat_scan_df['Application Number'] = ''

    final_demat_scan_df.to_excel('Demat Scan.xlsx', index=False)
    return final_unique_df

# Function to create the Demat.txt file
def create_demat_txt(final_unique_df):
    demat_txt_df = final_unique_df[['Policy Number']]
    demat_txt_df.to_csv('Demat.txt', index=False, header=False, sep='\t')

# Main function to execute all steps
def main():
    # 1. Read Files
    CCD_OP_DF, TEBT_OP_DF, OPS_OP_DF, NBEMAIL_DF, DematScan_DF, MIS_DF = read_excel_files()

    # 2. Handle Duplicates and Prioritization
    combined_df = handle_duplicates(NBEMAIL_DF, CCD_OP_DF, TEBT_OP_DF)

    # 3. Append Demat Scan Report
    final_combined_df = append_demat_scan_report(combined_df, DematScan_DF)

    # 4. Append Online OPS Data
    final_output_df = append_online_ops_data(final_combined_df, OPS_OP_DF)

    # 5. Perform Lookup with MIS
    unmatched_cases_df = perform_lookup_mis(final_output_df, MIS_DF)

    # 6. Remove Duplicates and Save Demat Scan Output
    final_unique_df = create_demat_scan_output(final_output_df, unmatched_cases_df)

    # 7. Create Demat.txt File
    create_demat_txt(final_unique_df)

    print("All processing completed successfully!")


if __name__ == "__main__":
    main()
