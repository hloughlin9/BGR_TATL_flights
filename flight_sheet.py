import gspread
from pandas import DataFrame

# Loading the spreadsheet.
def get_sheet():
    gc = gspread.service_account(r"C:\Users\henry\bgr-tatl-flights-1cf26373c7fd.json")
    df_sheet = gc.open("BGR_TATL_flights")
    df_worksheet = df_sheet.get_worksheet(0)
    df = DataFrame(df_worksheet.get_all_values()[1:])
    df.columns = df_worksheet.get_all_values()[0]
    return df, df_worksheet
