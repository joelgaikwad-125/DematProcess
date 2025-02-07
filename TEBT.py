import pandas as pd
from datetime import datetime
import os

def process_tebt_data():
    base_path = r"C:\Users\ADMIN\Desktop\GIT\Input_Files"
    dmat_file = os.path.join(base_path, "DMAT_DETAILS_20241107.csv")
    mis_xlsx_file = os.path.join(base_path, "MIS.xlsx")
    mis_csv_file = os.path.join(base_path, "MIS_converted.csv")
    eia_output_file = os.path.join(base_path, "EIA_No_Found_TEBT.csv")
    tebt_output_file = os.path.join(base_path, "TEBT_Data_Output.csv")
    
    try:
        df_dmat = pd.read_csv(dmat_file, dtype=str)
    except Exception as e:
        print(f"Error reading {dmat_file}: {e}")
        return
    
    df_dmat = df_dmat.assign(
        POLICY_NO=df_dmat['POLICY_NO'].fillna(""),
        DEMAT_ACCOUNT_NO=df_dmat['DEMAT_ACCOUNT_NO'].fillna(""),
        POS_APPLICATION_NO=df_dmat['POS_APPLICATION_NO'].fillna("")
    )
    
    df_dmat = df_dmat[(df_dmat['POLICY_NO'].str.strip() != "") & (~df_dmat['POLICY_NO'].str.startswith('9'))].copy()
    
    invalid_sequences = {"1234567890123", "1111111111111", "1000000000000", "1111111000000", "9999999999999", "1111110000001"}
    
    mask_eia = (
        df_dmat['DEMAT_ACCOUNT_NO'].str.match(r'^\d{13}$') &
        (df_dmat['DEMAT_ACCOUNT_NO'] != df_dmat['POS_APPLICATION_NO']) &
        df_dmat['DEMAT_ACCOUNT_NO'].str[0].isin(["1", "2", "3", "4", "5"]) &
        ~df_dmat['DEMAT_ACCOUNT_NO'].isin(invalid_sequences) &
        ~df_dmat['DEMAT_ACCOUNT_NO'].str.startswith("501") &
        (
            ((df_dmat['DEMAT_ACCOUNT_NO'].str[0] == '1') & (df_dmat['DEMAT_ACCOUNT_NO'].str[:5] == "10000")) |
            ((df_dmat['DEMAT_ACCOUNT_NO'].str[0] == '2') & (df_dmat['DEMAT_ACCOUNT_NO'].str[:5] == "20000")) |
            (df_dmat['DEMAT_ACCOUNT_NO'].str[0].isin(["3", "4", "5"]))
        )
    )
    
    df_eia_found = df_dmat[mask_eia].copy()
    current_date = datetime.now().strftime("%Y-%m-%d")
    df_eia_found = df_eia_found.assign(DATE=current_date, SOURCE="TEBT", PRODUCT_CODE="NSDL", IR="", TYPE="", PAN_UID="", E_IA_No="")
    df_eia_found.to_csv(eia_output_file, index=False)
    print(f"Saved EIA file with {len(df_eia_found)} rows to {eia_output_file}.")
    
    df_dmat = df_dmat[~mask_eia].copy()
    df_dmat.to_csv(dmat_file, index=False)
    print(f"Updated DMAT_DETAILS file after removing EIA rows.")
    
    try:
        df_mis = pd.read_excel(mis_xlsx_file, dtype=str)
        df_mis.to_csv(mis_csv_file, index=False)
        print(f"Converted MIS file from XLSX to CSV at {mis_csv_file}.")
    except Exception as e:
        print(f"Error reading {mis_xlsx_file}: {e}")
        return
    
    df_mis = df_mis.assign(Contract_No=df_mis['Contract No'].fillna(""))
    df_merge_mis = df_dmat.merge(df_mis[['Contract_No']], left_on='POLICY_NO', right_on='Contract_No', how='left', indicator=True)
    df_final = df_merge_mis[df_merge_mis['_merge'] == 'left_only'].drop(columns=['Contract_No', '_merge'])
    df_final.to_csv(tebt_output_file, index=False)
    print(f"Saved final output with {len(df_final)} rows to {tebt_output_file}.")

if __name__ == "__main__":
    process_tebt_data()
