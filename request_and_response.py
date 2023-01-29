import pandas as pd
import requests
from airportsdata import load


# Auth
key = "##### key #####"
url = "https://aeroapi.flightaware.com/aeroapi/"
airport = 'KBGR'
payload = {'max_pages': 1}
auth_header = {'x-apikey':key}


# Fetching airportsdata for loading in countries. Default argument is ICAO; get IATAs using load("IATA").
icaos = load()


class Request:
    """
    Returns a response object with the up-to-date flights.
    ...
    Attributes:
    -----------
    type string:
        Specify whether arrival or departure request.
    -----------
    returns:
        Response of flights in the given time period matching the relevant condition.
        """

    def __init__(self, type=None):
        self.key = key
        self.payload = payload
        self.url = url
        self.response = requests.get(self.url + f"airports/{airport}/flights",
            headers=auth_header).json()


        if type is not None:

            if type == "A":
                self.df = self.response['arrivals']
            elif type == "D":
                self.df = self.response['departures']
            else:
                raise ValueError("Must choose 'A' or 'D'.")

        else:
            raise ValueError("Must specify arrivals or departures.")


req_a = Request(type="A").df
req_d = Request(type="D").df


# Getting the relevant records for arrivals and departure are a bit of a beast.

a_idents = []
a_origin_icaos = []
a_destination_icaos = []
a_origins = []
a_destinations = []
a_offs = []
a_ons = []
a_types = []
a_origin_countries = []
a_destination_countries = []


for i in range(len(req_a)):
    try:
        a_origin_icaos.append(req_a[i]['origin']['code'])
    except (KeyError, TypeError):
        a_origin_icaos.append("None")
    try:
        a_destination_icaos.append(req_a[i]['destination']['code'])
    except (KeyError, TypeError):
        a_destination_icaos.append("None")
    try:
        a_origins.append(req_a[i]['origin']['code_iata'])
    except (KeyError, TypeError):
        a_origins.append("None")
    try:
        a_destinations.append(req_a[i]['destination']['code_iata'])
    except TypeError:
        a_destinations.append("None")
    try:
        a_types.append(req_a[i]['aircraft_type'])
    except (KeyError, TypeError):
        a_types.append("None")
    try:
        a_offs.append(req_a[i]['actual_off'])
    except (KeyError, TypeError):
        a_offs.append("None")
    try:
        a_ons.append(req_a[i]['actual_on'])
    except (KeyError, TypeError):
        a_ons.append("None")
    try:
        a_idents.append(req_a[i]['ident'])
    except (KeyError, TypeError):
        a_idents.append("None")
        

a_df = pd.DataFrame([a_ons, a_idents, a_origin_icaos, a_destination_icaos, a_origins, a_destinations, a_types]).transpose()
a_df.columns = ['Date', 'ident','origin_icao','destination_icao',"origin","destination","type"]


for i in a_df['origin_icao']:
    try:
        a_origin_countries.append(icaos[i]['country'])
    except (TypeError, KeyError):
        a_origin_countries.append("None")

        
for i in a_df['destination_icao']:
    try:
        a_destination_countries.append(icaos[i]['country'])
    except (TypeError, KeyError):
        a_destination_countries.append("None")

        
a_df['origin_country'] = a_origin_countries
a_df['destination_country'] = a_destination_countries


d_idents = []
d_origin_icaos = []
d_destination_icaos = []
d_origins = []
d_destinations = []
d_offs = []
d_ons = []
d_types = []
d_origin_countries = []
d_destination_countries = []


for i in range(len(req_d)):
    try:
        d_origin_icaos.append(req_d[i]['origin']['code'])
    except (KeyError, TypeError):
        d_origin_icaos.append("None")
    try:
        d_destination_icaos.append(req_d[i]['destination']['code'])
    except (KeyError, TypeError):
        d_destination_icaos.append("None")
    try:
        d_origins.append(req_d[i]['origin']['code_iata'])
    except (KeyError, TypeError):
        d_origins.append("None")
    try:
        d_destinations.append(req_d[i]['destination']['code_iata'])
    except TypeError:
        d_destinations.append("None")
    try:
        d_types.append(req_d[i]['aircraft_type'])
    except (KeyError, TypeError):
        d_types.append("None")
    try:
        d_offs.append(req_d[i]['actual_off'])
    except (KeyError, TypeError):
        d_offs.append("None")
    try:
        d_ons.append(req_d[i]['actual_on'])
    except (KeyError, TypeError):
        d_ons.append("None")
    try:
        d_idents.append(req_d[i]['ident'])
    except (KeyError, TypeError):
        d_idents.append("None")

        
d_df = pd.DataFrame([d_offs, d_idents, d_origin_icaos, d_destination_icaos, d_origins, d_destinations, d_types]).transpose()
d_df.columns = ['Date', 'ident','origin_icao','destination_icao',"origin","destination","type"]


for i in d_df['origin_icao']:
    try:
        d_origin_countries.append(icaos[i]['country'])
    except (TypeError, KeyError):
        d_origin_countries.append("None")

        
for i in d_df['destination_icao']:
    try:
        d_destination_countries.append(icaos[i]['country'])
    except (TypeError, KeyError):
        d_destination_countries.append("None")
        

d_df['origin_country'] = d_origin_countries
d_df['destination_country'] = d_destination_countries


bgr = pd.concat([a_df, d_df], axis=0).reset_index(drop=True).sort_values(by="Date")
