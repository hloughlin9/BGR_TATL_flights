import gspread
from pandas import DataFrame

# Loading the spreadsheet.
def get_sheet():
    gc = gspread.service_account("##### credentials_path.json #####")
    df_sheet = gc.open("BGR_TATL_flights")
    df_worksheet = df_sheet.get_worksheet(0)
    df = DataFrame(df_worksheet.get_all_values()[1:])
    df.columns = df_worksheet.get_all_values()[0]
    return df, df_worksheet
