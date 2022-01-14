import pandas as pd

cols = ['FA','SS']

ac = pd.read_csv("C:\\Users\\henry\\PycharmProjects\\BGR_TATL_flights\\data\\AC.csv", names=cols)
ac_dict = dict(zip(ac['FA'],ac['SS']))

al = pd.read_csv("C:\\Users\\henry\\PycharmProjects\\BGR_TATL_flights\\data\\AL.csv", names=cols)
al_dict = dict(zip(al['FA'],al['SS']))

codes = pd.read_csv("C:\\Users\\henry\\PycharmProjects\\BGR_TATL_flights\\data\\ICAOIATA.csv", names=cols)
code_dict = dict(zip(codes['FA'],codes['SS']))

apc = pd.read_csv("C:\\Users\\henry\\PycharmProjects\\BGR_TATL_flights\\data\\APC.csv")
apc_dict = dict(zip(apc['IATA'],apc['Country']))
