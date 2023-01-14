import warnings
import re
import pandas as pd
import time
import dateutil
from gspread_dataframe import set_with_dataframe
from datetime import datetime as dt, timezone
from sys import exit
from airportsdata import load
from request_and_response import Request, ResponseToDataFrame
from flight_sheet import get_sheet
warnings.filterwarnings("ignore")

# January 2023 update

# The main objective for the program is to automate the addition of flights to an existing spreadsheet.
# Previously, there were nine fields to be filled out per observation:
# * Date (if none, current datetime date entered)
# * Airline (if applicable)
# * Flight # (if applicable)
# * Aircraft type (required)
# * Origin IATA code
# * Origin Country
# * Destination IATA code
# * Destination Country
# * Direction

# With this program, we will scrape the FlightAware API to see if there are any new transatlantic flights to add.
# If there are no flights to add, the program exits and nothing changes.
# If there are flights to add, it will format them properly by:

# * Reducing the departure/arrival times to strf dates
# * Splitting the ICAO identifier into an Airline and Flight # (if applicable)
# * Recording aircraft type
# * Converting ICAO codes to IATA codes via dictionary
# * Adding origin and destination countries (derived from IATA codes via dictionary)
# * Direction based on origin country (e.g. If flight in US, it's "E" for East; otherwise, "W" for West.)
# In the interest of cardinality, we have also added an ID field to provide a primary key.

# A couple of housekeeping items. We are using Eastern Time, and the list of columns below are relevant for the use of
# airline and aircraft dictionaries.
et = dateutil.tz.gettz("America/New_York")

dict_cols = ['FA', 'SS']

# Below, two CSVs that we import as dictionaries, essentially.

# One maps airline ICAO identifiers (e.g. BAW for British Airways)
# to airline names.
ac = pd.read_csv("AC.csv", names=dict_cols)
ac_dict = dict(zip(ac['FA'], ac['SS']))

# Another maps aircraft ICAO identifiers (e.g. B744 for Boeing 747-400)
# to aircraft names.
al = pd.read_csv("AL.csv", names=dict_cols)
al_dict = dict(zip(al['FA'], al['SS']))


# Loading the spreadsheet.
df, df_worksheet = get_sheet()


# Get the initial length.
initial_length = len(df)


# Previous flights added 6/27/2022
prev_flights = set(df['ID'])


# The Request class makes the request to the FlightAware AeroAPI. We get both arrivals and departures here.
req_a = Request(type="A").df
req_d = Request(type="D").df


# The ResponseToDataFrame class converts the returned response (above) into a DataFrame.
req_a_df = ResponseToDataFrame(req_a).df
req_d_df = ResponseToDataFrame(req_d).df


# We do not want any flights where there is no origin or destination ICAO code.
req_a_df_final = req_a_df[req_a_df['origin_icao'] != ""]
req_d_df_final = req_d_df[req_d_df['destination_icao'] != ""]


# Stack the two DataFrames.
bgr = pd.concat([req_a_df_final, req_d_df_final], axis=0).reset_index(drop=True)


# Get the current date and time.
now = dt.now(tz=et).strftime("%Y-%m-%d %H:%M:%S")
print(f"Flights pulled from FlightAware AeroAPI query at {now}.")


# A list of dates. Since eastbound transatlantic flights may depart on one day and arrive on another — this is
# theoretically the case with some late-departing westbound transatlantic flights, we need to be sure that we
# are pulling the correct date: when it arrived or departed.
dates = []

for i in range(len(bgr)):
    if bgr['origin_icao'][i] == "KBGR":
        dates.append(bgr['off'][i])
    else:
        dates.append(bgr['on'][i])


def utc_to_local(utc_dt):

    """
    Quick lambda function to convert UTC to local time (ET).

    Parameters
    ----------
    utc_dt: The date provided by the API pull in UTC.

    Returns
    -------
    Local time.
    """

    return utc_dt.replace(tzinfo=timezone.utc).astimezone(tz=et)


# A couple of date transformations to convert the initial dates into the correct ones.
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


# Drop the null aircraft 1/8/2022
bgr = bgr[bgr['type'].notna()]


# We want to filter out flights that are entirely arriving and departing from the US (starting with "K"), Canada
# (starting with "C"), Mexico (starting with "M"), Greenland (starting with "BG"), or the Caribbean (starting with "T").
# There are other possible airports but most can be dealt with ad hoc.
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


# All airport data, pulled by ICAO code from airportsdata. Default argument is ICAO; get IATAs using load("IATA").
icaos = load()

# The ICAO-IATA and ICAO-country maps via airportsdata necessitated two fewer dictionaries than before. 6/25/2022
bgr['origin country'] = bgr['origin_icao'].apply(lambda x: icaos[x]['country'])
bgr['destination country'] = bgr['destination_icao'].apply(lambda x: icaos[x]['country'])

# ID serialization.
bgr['id'] = bgr['Date'].astype(str) + bgr['airline_sym'].astype(str) + bgr['flight'].astype(str)
bgr['id'] = bgr['id'].str.replace("-", "")
bgr['id'] = bgr['id'].str[2:]
bgr['id'] = bgr['id'].str.replace("nan", "")

# Replacing None flight numbers with nothing.
bgr['id'] = bgr['id'].str.replace("None", "")
bgr['flight'] = bgr['flight'].str.replace("None", "")

# An ordered list to use. This will change prior to the upload, but we will need certain fields for subsetting the data.
ordered = ['ident', 'origin_icao', 'destination_icao', 'Origin', 'Destination', 'origin_name', 'destination_name',
           'off', 'on', 'Type', 'Date', 'airline_sym', 'Airline', 'Flight', 'Origin Country', 'Destination Country',
           'ID']


# Set the columns as the ordered list we just defined.
bgr.columns = ordered


# Getting the direction of the flight based on where the origin country is.
bgr['Direction'] = ["E" if i == "US" else "W" for i in bgr["Origin Country"]]


# Logic to include only new flights. 6/27/2022
bgr = bgr[~bgr['ID'].isin(prev_flights)]


# Get the length of the bgr DataFrame.
bgr_length = len(bgr)


# Be sure that no flights are included without an origin or destination.
bgr = bgr[(bgr['Origin'] != None) & (bgr['Destination'] == None)]


# Drop any records with both the origin and destination having the same country.
bgr = bgr[~((bgr['Origin Country'] == "US") & (bgr['Destination Country'] == "US"))]


# These are the final columns to be used by the DataFrame.
final_columns = ['ID', 'Date', 'Airline', 'Flight', 'Origin', 'Destination',
                 'Origin Country', 'Destination Country', 'Direction']


# Order the DataFrame by the final columns order.
bgr = bgr[final_columns]

# Drop duplicate code chained 13:01 1/1/2022
df_final = pd.concat([df, bgr], axis=0).reset_index(drop=True)

# Sort values isolated 10:34 2/5/2022
df_final = df_final.sort_values(by=['Date'])

# Get the end length of the DataFrame.
df_end_length = len(df_final)


# Bool length calc rebuilt 6/25/2022
if initial_length == df_end_length:
    print("No flights to add right now. Program exiting.")
    time.sleep(4)
    exit()
else:
    pass


# Print flights logic created 5/26/2022, amended 5/28/2022, replaced 6/4/2022
print(f"{bgr_length} flights added. {df_end_length} flights total\n")
print("Flight(s) added:\n")
print(bgr)


# Set the worksheet as the new version.
set_with_dataframe(df_worksheet, df_final)

# Sleep logic set to 4 19:02 2/12/2022
time.sleep(4)

# Exit given sys import for structural integrity 6/18/2022
exit()