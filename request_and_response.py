from pandas import DataFrame
import requests

# Auth
key = "##### key #####"
url = "https://aeroapi.flightaware.com/aeroapi/"
airport = 'KBGR'
payload = {'max_pages': 1}
auth_header = {'x-apikey':key}

class Request:
    """
    Returns a departure object.
    ...
    Attributes:
    -----------
    type: string
        Specify whether arrival or departure request.
    -----------
    returns:
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


def get_rows(response, col):
    return [response[i][col] for i in range(len(response))]

def get_rows_airport(response, col, type):
    return [response[i][col][type] for i in range(len(response))]


class ResponseToDataFrame:

    """
    Returns dataframe given a json response.
    ...
    Attributes:
    -----------
    response object:
        Response object from json.
    -----------
    returns:
        DataFrame of response object with necessary columns.
    """

    def __init__(self, response):
        self.ident = get_rows(response, "ident")
        self.origin_icao = get_rows_airport(response, "origin", "code_icao")
        self.destination_icao = get_rows_airport(response, "destination", "code_icao")
        self.origin = get_rows_airport(response, "origin", "code_iata")
        self.destination = get_rows_airport(response, "destination", "code_iata")
        self.off = get_rows(response, "actual_off")
        self.on = get_rows(response, "actual_on")
        self.type = get_rows(response, "aircraft_type")
        self.df = DataFrame([self.ident, self.origin, self.destination, self.off, self.on,
                             self.origin_icao, self.destination_icao, self.type]).transpose()
        self.df.columns = ['ident','Origin','Destination','out','in','origin_icao','destination_icao', 'Type']