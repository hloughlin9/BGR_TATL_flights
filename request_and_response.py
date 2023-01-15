from pandas import DataFrame
import requests

# Auth
key = "GpEfxxz7c8Gd5AjWW3Nt5Uwd55EGAUjv"
url = "https://aeroapi.flightaware.com/aeroapi/"
airport = 'KBGR'
payload = {'max_pages': 1}
auth_header = {'x-apikey':key}

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


def drop_nones(response):
    """

    Parameters
    ----------
    response object:
        Response object from json.

    Returns:
        Response object minus records without ICAO or IATA codes.
    -------

    """
    response_range_length = range(len(response))
    return [response[i] for i in response_range_length if (response[i]['ident_icao'] != None) &
            (response[i]['ident_icao'] != None)]

def get_rows(response, col):
    """
    Retrieves rows for non-airport objects within the response.

    Parameters
    ----------
    response object:
        Response object from json.
    col:
        Column with which to get the rows.

    Returns:
        Individual rows for DataFrame.
    -------

    """
    return [response[i][col] for i in range(len(response))]

def get_rows_airport(response, col, type):
    """
    Retrieves rows for airport objects which have unique characteristics compared to other non-airport fields.

    Parameters
    ----------
    response object:
        Response object from json.
    col string:
        Type of movements to retrieve; should be "origin" or "destination".
    type string:
        Field name for airport characteristic to be returned (e.g. code_icao).
    Returns:
        Individual rows for DataFrame.
    -------

    """
    return [response[i][col][type] for i in range(len(response))]


class ResponseToDataFrame:

    """
    Returns filtered response as DataFrame.
    """

    def __init__(self, response):
        self.ident = get_rows(response, "ident")
        self.origin_icao = get_rows_airport(response, "origin", "code_icao")
        self.destination_icao = get_rows_airport(response, "destination", "code_icao")
        self.origin = get_rows_airport(response, "origin", "code_iata")
        self.destination = get_rows_airport(response, "destination", "code_iata")
        self.origin_name = get_rows_airport(response, "origin", "name")
        self.destination_name = get_rows_airport(response, "destination", "name")
        self.off = get_rows(response, "actual_off")
        self.on = get_rows(response, "actual_on")
        self.type = get_rows(response, "aircraft_type")
        self.df = DataFrame([self.ident, self.origin_icao, self.destination_icao, self.origin, self.destination,
                             self.origin_name, self.destination_name, self.off, self.on,
                             self.type]).transpose().reset_index(drop=True)
        self.df.columns = ['ident','origin_icao','destination_icao',"origin","destination","origin_name","destination_name","off","on","type"]
