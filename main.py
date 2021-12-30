from src import Arrivals, Departures
from gs import df, df_ws
from dicts import ac_dict, al_dict, apc_dict, code_dict
from datetime import datetime as dt
from sklearn.preprocessing import LabelEncoder
from imblearn.ensemble import BalancedRandomForestClassifier
from gspread_dataframe import set_with_dataframe
import re
import pandas as pd
import warnings
warnings.filterwarnings("ignore")

now = dt.now()

print(f"Program run at {now}.")

arr = Arrivals().df
dep = Departures().df
bgr = arr.append(dep)

bgr['arrivaltime'] = [bgr['estimatedarrivaltime'] if 0 else i for i in bgr['actualarrivaltime']]
bgr = bgr.drop(['estimatedarrivaltime','actualarrivaltime'], axis=1)

# Here we will capture the identifiers.
idents_a = []
idents_b = []

for col in bgr['ident']:
    idents_a.append(re.split("(\d+)", col)[0])

for col in bgr['ident']:
    try:
        idents_b.append(re.split("(\d+)", col)[1])
    except IndexError:
        idents_b.append("None")

bgr['Airline'] = pd.Series(idents_a).map(al_dict)
bgr['Flight'] = idents_b
bgr['Type'] = bgr['aircrafttype'].map(ac_dict)

bgr = bgr.drop(["aircrafttype","ident"], axis=1)
bgr['Origin'] = bgr['origin'].str.strip()
bgr['Destination'] = bgr['destination'].str.strip()

bgr = bgr[((bgr['Origin'].str[0] != "K") & (bgr['Origin'].str[0] != "C") & (bgr['Origin'].str[1] != " ") & (bgr['Origin'].str[0] != "M")) | ((bgr['Destination'].str[0] != "K") & (bgr['Destination'].str[0] != "C") & (bgr['Destination'].str[1] != " ") & (bgr['Destination'].str[0] != "M"))]

bgr['Origin'] = bgr['Origin'].map(code_dict)
bgr['Destination'] = bgr['Destination'].map(code_dict)

bgr['Origin Country'] = bgr['Origin'].map(apc_dict)
bgr['Destination Country'] = bgr['Destination'].map(apc_dict)

bgr["Date"] = bgr["actualdeparturetime"].apply(lambda x: dt.fromtimestamp(x).strftime("%Y-%m-%d"))

bgr = bgr.drop(["actualdeparturetime","origin","destination","originName","destinationName","originCity","destinationCity","arrivaltime"], axis=1)

if len(bgr) == 0:
    print("No flights to add.")
    exit()
else:
    pass

# No medical flights.
bgr = bgr[(bgr['Flight'] != "901") | (bgr['Airline'] != "N")]

# ID serialization.
bgr['ID'] = bgr['Date'].astype(str) + bgr['Airline'].astype(str) + bgr['Flight'].astype(str)
bgr['ID'] = bgr['ID'].str.replace("-", "")
bgr['ID'] = bgr['ID'].str[2:]
bgr['ID'] = bgr['ID'].str.replace("nan", "")


X, y = df[['Origin Country']], df['Direction']

le = LabelEncoder()

X = le.fit_transform(X.astype(str)).reshape(-1,1)

if len(bgr) != 1:
    X_test = le.transform(bgr['Origin Country'].astype(str)).reshape(-1,1)
else:
    X_test = le.transform(bgr['Origin Country'].astype(str)).reshape(1,-1)

model = BalancedRandomForestClassifier()

model.fit(X, y)

predictions = model.predict(X_test)

bgr['Direction'] = predictions

bgr = bgr[['ID','Date','Airline','Flight','Type','Origin','Origin Country','Destination','Destination Country','Direction']]

df = df.append(bgr).sort_values(by=['Date']).reset_index(drop=True)

# Last fix 18:30 12/29/2021
set_with_dataframe(df_ws, df)

print(f"{len(bgr)} flights added. {len(df)} flights total")

exit()