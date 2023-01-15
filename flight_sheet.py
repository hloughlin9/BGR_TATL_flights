import gspread
import os
from pandas import DataFrame

os.chdir("##### creds_dir #####")

# Loading the spreadsheet.
def get_sheet():
    gc = gspread.service_account("##### creds_path.json #####")
    df_sheet = gc.open("BGR_TATL_flights")
    df_worksheet = df_sheet.get_worksheet(0)
    df = DataFrame(df_worksheet.get_all_values()[1:])
    df.columns = df_worksheet.get_all_values()[0]
    return df, df_worksheet
