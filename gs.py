import gspread
import pandas as pd

gc = gspread.service_account()
df_sheet = gc.open("BGR_TATL_flights")
df_ws = df_sheet.get_worksheet(0)
df = pd.DataFrame(df_ws.get_all_values())
df.columns = df.iloc[0,:]
df = df.iloc[1:,:]