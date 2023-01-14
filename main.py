import warnings
import re
import pandas as pd
import time
import dateutil
from gspread_dataframe import set_with_dataframe
from datetime import datetime as dt, timezone as tz
from sys import exit
from airportsdata import load
from request_and_response import Request, ResponseToDataFrame
from flight_sheet import get_sheet

# January 2023 update

# The main objective for the program is to automate the addition
# of flights to an existing spreadsheet.
# Previously, there were nine fields to be filled out per observation:
# Date (if none, current datetime date entered)
# Airline (if applicable)
# Flight # (if applicable)
# Aircraft type (required)
# Origin IATA code
# Origin Country
# Destination IATA code
# Destination Country
# Direction
#
# With this program, we will scrape the FlightAware API to see
# if there are any new transatlantic flights to add to our spreadsheet.
# If there are no flights to add, the program exits and nothing changes.
# If there are flights to add, it will format them properly by:
# Reducing the departure/arrival times to strf dates
# Splitting the ICAO identifier into an Airline and Flight # (if applicable)
# Recording aircraft type
# Converting ICAO codes to IATA codes via dictionary
# Adding origin and destination countries (derived from IATA codes
# via dictionary)
# Direction based on origin country (e.g. If flight
# in US, it's "E" for East; otherwise, "W" for West.)
# In the interest of cardinality, we have also added an
# id field to provide a singular field that can function as a primary key.

warnings.filterwarnings("ignore")

et = dateutil.tz.gettz("America/New_York")

pd.options.display.max_columns = 15

# Two CSVs that we import as dictionaries, essentially.
# One maps airline ICAO identifiers (e.g. BAW for British Airways)
# to airline names.
# Another maps aircraft ICAO identifiers (e.g. B744 for Boeing 747-400)
# to aircraft names.
cols = ['FA', 'SS']

ac = pd.read_csv("AC.csv", names=cols)
ac_dict = dict(zip(ac['FA'], ac['SS']))

al = pd.read_csv("AL.csv", names=cols)
al_dict = dict(zip(al['FA'], al['SS']))

# Loading the spreadsheet.
df, df_worksheet = get_sheet()

# Get the initial length.
init_len = len(df)

# Previous flights added 6/27/2022
prev_flights = set(df['ID'])

req_a = Request(type="A").df
req_d = Request(type="D").df

req_a_df = ResponseToDataFrame(req_a).df
req_d_df = ResponseToDataFrame(req_d).df

req_a_df_final = req_a_df[req_a_df['origin_icao'] != ""]
req_d_df_final = req_d_df[req_d_df['destination_icao'] != ""]


bgr = pd.concat([req_a_df_final, req_d_df_final], axis=0).reset_index(drop=True)


now = dt.now(tz=et).strftime("%Y-%m-%d %H:%M:%S")
print(f"Flights pulled from FlightAware AeroAPI query at {now}.")

dates = []

for i in range(len(bgr)):
    if bgr['origin_icao'][i] == "KBGR":
        dates.append(bgr['off'][i])
    else:
        dates.append(bgr['on'][i])


def utc_to_local(utc_dt):
    return utc_dt.replace(tzinfo=tz.utc).astimezone(tz=et)


bgr['Date'] = pd.to_datetime(dates)
bgr['Date'] = bgr['Date'].apply(lambda x: utc_to_local(x))
bgr['Date'] = bgr['Date'].apply(lambda x: x.strftime("%Y-%m-%d"))

# This ensures that our ICAO identifiers all have 4 letters.
bgr = bgr[(bgr['origin_icao'].str.len() == 4) &
          (bgr['destination_icao'].str.len() == 4)].reset_index(drop=True)


# Now that we have our arrival and departures together (for a given pull),
# we will capture the identifiers.
idents_a = []
idents_b = []

# This grabs the airline (ICAO).
for col in bgr['ident']:
    idents_a.append(re.split("(\\d+)", col)[0])

# Excluding singular letter-based identifiers without any numeric values,
# we'll be able to grab most flight numbers here.
for col in bgr['ident']:
    try:
        idents_b.append(re.split("(\\d+)", col)[1])
    except IndexError:
        idents_b.append("None")

# Converting the airline ICAO codes from the API pull to a dictionary of
# codes listed as they are in the spreadsheet.
bgr['airline_sym'] = pd.Series(idents_a)
bgr['airline_sym'].fillna("None", inplace=True)
bgr['airline'] = bgr['airline_sym'].map(al_dict)
bgr['flight'] = idents_b
bgr['type'] = bgr['type'].map(ac_dict)

# Navy/USAF logic included 6/11/2022. There may be the odd C-130 or C-5
# in the case of USAF but that is OK and can be dealt with ad hoc during
# ongoing validation.
bgr['type'].loc[bgr['airline'] == "US Navy"] = "Boeing 737-700"
bgr['type'].loc[bgr['airline'] == "US Air Force"] = "Boeing C-17 Globemaster"

# Drop the null aircraft 1/8/2022
bgr = bgr[bgr['type'].notna()]


# We want to filter out flights that are entirely arriving and departing
# from the US (starting with "K"), Canada (starting with "C"), Mexico
# (starting with "M"), and
# Greenland (starting with "BG"). There are other possible airports
# but most can be dealt with ad hoc.
# "BG" and " " on bgr['Type'][0] checks 12:10 12/31/2021. (Greenland)
# "T" checks on bgr['Origin'][0] 4/23/2022 (Carribean/)
# Medical filters moved to main filter 6/29/2022
bgr = bgr[(((bgr['origin_icao'].str[0] != "K") & (bgr['origin_icao'].str[0] != "C")
            & (bgr['origin_icao'].str[1] != " ") & (bgr['origin_icao'].str[0] != "M") &
            (bgr['origin_icao'].str[0:2] != "BG") & (bgr['origin_icao'].str[0] != "T"))
           | ((bgr['destination_icao'].str[0] != "K") &
              (bgr['destination_icao'].str[0] != "C") &
              (bgr['destination_icao'].str[1] != " ") &
              (bgr['destination_icao'].str[0] != "M") &
              (bgr['destination_icao'].str[0:2] != "BG") &
              (bgr['destination_icao'].str[0] != "T") &
              ((bgr['flight'] != "901")
              | (bgr['airline'] != "N"))
              ))]

# All airport data, pulled by ICAO code from airportsdata.
# Default argument is ICAO, we could start with IATAs using load("IATA")
icaos = load()

# The ICAO-IATA and ICAO-country maps via airportsdata necessitated two fewer
# dictionaries than before. 6/25/2022
bgr['origin country'] = bgr['origin_icao'].apply(lambda x: icaos[x]['country'])
bgr['destination country'] = bgr['destination_icao'].apply(lambda x: icaos[x]['country'])

# id serialization.
bgr['id'] = bgr['Date'].astype(str) + bgr['airline_sym'].astype(str) \
            + bgr['flight'].astype(str)
bgr['id'] = bgr['id'].str.replace("-", "")
bgr['id'] = bgr['id'].str[2:]
bgr['id'] = bgr['id'].str.replace("nan", "")

# Replacing None flight numbers with nothing.
bgr['id'] = bgr['id'].str.replace("None", "")
bgr['flight'] = bgr['flight'].str.replace("None", "")

ordered = ['ident', 'origin_icao', 'destination_icao', 'Origin', 'Destination',
       'origin_name', 'destination_name', 'off', 'on', 'Type', 'Date',
       'airline_sym', 'Airline', 'Flight', 'Origin Country',
       'Destination Country', 'ID']

bgr.columns = ordered

bgr['Direction'] = ["E" if i == "US" else "W" for i in bgr["Origin Country"]]


# Logic to include only new flights 6/27/2022
bgr = bgr[~bgr['ID'].isin(prev_flights)]

bgr_len = len(bgr)

bgr.columns = [o for o in ordered] + ['Direction']


# Delete any records with both the origin and destination having the same country.
bgr = bgr[~((bgr['Origin Country'] == "US") & (bgr['Destination Country'] == "US"))]

# Drop duplicate code chained 13:01 1/1/2022
df_final = pd.concat([df, bgr], axis=0).reset_index(drop=True)

# Sort values isolated 10:34 2/5/2022
df_final = df_final.sort_values(by=['Date'])

# Adj 5/27/2022
df_end_len = len(df_final)

# Bool length calc rebuilt 6/25/2022
if init_len == df_end_len:
    print("No flights to add. Program exiting.")
    time.sleep(4)
    exit()
else:
    pass


# Print flights logic created 5/26/2022, amended 5/28/2022, replaced 6/4/2022
print(f"{bgr_len} flights added. {df_end_len} flights total")
print()
print("Flight(s) Added:")
print()
print(bgr)

# Set the worksheet as the new version.
set_with_dataframe(df_worksheet, df_final)

# Sleep logic set to 4 19:02 2/12/2022
time.sleep(4)

# Exit given sys import for structural integrity 6/18/2022
exit()