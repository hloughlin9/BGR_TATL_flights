import warnings
import re
import pandas as pd
import time
from dateutil import tz
from gspread_dataframe import set_with_dataframe
from datetime import datetime as dt
from sys import exit
from request_and_response import bgr
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

# A few housekeeping items. We are using Eastern Time, and the list of columns below are relevant for the use of
# airline and aircraft dictionaries. We also want there to be 10 columns viewable.
et = tz.gettz("America/New_York")
dict_cols = ['FA', 'SS']
pd.options.display.max_columns = 10


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


# Get the current date and time.
now = dt.now(tz=et).strftime("%Y-%m-%d %H:%M:%S")
print(f"Flights retrieved from FlightAware AeroAPI query at {now}.\n")


# A couple of date transformations to convert the initial dates into the correct ones.
bgr['Date'] = pd.to_datetime(bgr['Date'])
bgr['Date'] = bgr['Date'].dt.tz_convert("US/Eastern")


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
           | ((bgr['destination_icao'].str[0] != "K") & (bgr['destination_icao'].str[0] != "C") &
              (bgr['destination_icao'].str[1] != " ") & (bgr['destination_icao'].str[0] != "M") &
              (bgr['destination_icao'].str[:2] != "BG") & (bgr['destination_icao'].str[0] != "T") &
              ((bgr['flight'] != "901") | (bgr['airline'] != "N"))
              ))]


bgr['Date'] = bgr['Date'].apply(lambda x: dt.strftime(x, "%Y-%m-%d"))


# ID serialization.
bgr['id'] = bgr['Date'].astype(str) + bgr['airline_sym'].astype(str) + bgr['flight'].astype(str)
bgr['id'] = bgr['id'].str.replace("-", "")
bgr['id'] = bgr['id'].str[2:]
bgr['id'] = bgr['id'].str.replace("nan", "")


# Replacing None flight numbers with nothing.
bgr['id'] = bgr['id'].str.replace("None", "")
bgr['flight'] = bgr['flight'].str.replace("None", "")


# An ordered list to use. This will change prior to the upload, but we will need certain fields for subsetting the data.
ordered = ['Date', 'id', 'airline', 'flight', 'type', 'origin', 'origin_country', 'destination', 'destination_country']

# Set the columns as the ordered list we just defined.
bgr = bgr[ordered]

# Getting the direction of the flight based on where the origin country is.
bgr['Direction'] = ["E" if i == "US" else "W" for i in bgr["origin_country"]]

# Logic to include only new flights. 6/27/2022
bgr = bgr[~bgr['id'].isin(prev_flights)]

# Get the length of the bgr DataFrame.
bgr_length = len(bgr)


# Drop any records with both the origin and destination having the same country OR not being able to determine them.
bgr = bgr[~((bgr['origin_country'] == "US") & (bgr['destination_country'] == "US")) &
          ((bgr['origin_country'] != "None") & (bgr['destination_country'] != "None"))]


# Order the DataFrame by the final columns order.
bgr = bgr.rename(columns={"id":"ID", "airline":"Airline", "flight":"Flight", "type":"Type",
                          "origin":"Origin", "origin_country":"Origin Country", "destination":"Destination",
                          "destination_country":"Destination Country"})


# Drop duplicate code chained 13:01 1/1/2022
df_final = pd.concat([df, bgr], axis=0).reset_index(drop=True)


# Sort values isolated 10:34 2/5/2022
df_final = df_final.sort_values(by=['Date'])


# Get the end length of the DataFrame.
df_end_length = len(df_final)


# Bool length calc rebuilt 6/25/2022
if initial_length == df_end_length:
    print("No flights to add at this time.\nProgram closing.")
    time.sleep(4)
    exit()
else:
    pass


# Print flights logic created 5/26/2022, amended 5/28/2022, replaced 6/4/2022
print(f"{bgr_length} flights added. {df_end_length} flights total\n")
print(f"Flight(s) added to BGR_TATL_flights:\n{bgr}")


# Set the worksheet as the new version.
set_with_dataframe(df_worksheet, df_final)

# Sleep logic set to 4 19:02 2/12/2022
time.sleep(4)

# Exit given sys import for structural integrity 6/18/2022
exit()