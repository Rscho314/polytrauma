#!/usr/bin/env python3

# TODO: remove signals not in right time interval

import zipfile
import numpy as np
import pandas as pd
from io import BytesIO

with zipfile.ZipFile("polytrauma.zip", "r") as zf:
    datalist = [zf.read(f) for f in zf.namelist()]
    d = {name: BytesIO(datum)  for (name, datum) in zip(zf.namelist(), datalist)}
 
# get data into pandas
pandas_data = {name: (pd.read_excel(data, header=0, skiprows=6, usecols=4) if name != "polytrauma.xlsx" else pd.read_excel(data)) for (name, data) in d.items()}

# separate raw DANI files from custom file
data_to_aggregate = {name: data for (name, data) in pandas_data.items() if name != "polytrauma.xlsx"}

# remove 1st row
dani_data = {name:data.drop(data.index[0]) for (name, data) in data_to_aggregate.items()}

# base datasets
custom = pandas_data["polytrauma.xlsx"]
events = dani_data["polytrauma_duree.xlsx"]
rest = {name: data for (name, data) in dani_data.items() if name != "polytrauma_duree.xlsx"}
signal = pd.concat(rest.values(), ignore_index=True)
recrutement = events[events["Nom de l'événement"]=="Recrutement pulmonaire"]
time = events[events["Nom de l'événement"]!="Recrutement pulmonaire"]

# intermediate datasets
duration = time.groupby(["N°IPP", "Nom de l'événement"]).head(1)
intervention = duration[duration["Nom de l'événement"]=="Intervention"]
anesthesie = duration[duration["Nom de l'événement"]=="Anesthésie"]
signals = {k:v for k,v in signal.groupby(by="Nom du paramètre")}
signals_grouped = {name:data.groupby(by="N°IPP") for name,data in signals.items()}
varnames = ['freq_resp', 'hct', 'hb', 'lactate', 'ph', 'peep', 'ktart_dia', 'ktart_moy', 'ktart_sys', 'pni_dia', 'pni_moy', 'pni_sys', 'p_plateau', 'urines', 'tidal', 'vent_min']
signals_new_names = {nk:signals_grouped[k] for (k, nk) in zip(list(signals_grouped.keys()), varnames)}

def perc25(x):
    return np.percentile(x, 25)

def perc75(x):
    return np.percentile(x, 75)

signals_stats = {name:data.agg([np.mean, np.std, np.median, perc25, perc75, np.min, np.max, np.sum]).rename(columns={"mean":name+"_"+"mean", "std":name+"_"+"std", "median":name+"_"+"median", "perc25":name+"_"+"perc25", "perc75":name+"_"+"perc75", "amin":name+"_"+"min", "amax":name+"_"+"max", "sum":name+"_"+"sum"}) for (name,data) in signals_new_names.items()}

#results
res_time = pd.merge(intervention, anesthesie, on="N°IPP")
res_time = res_time.rename(columns={"Heure de début_x":"debut_chir", "Heure de fin_x":"fin_chir", "Durée(minutes)_x":"duree_chir","Heure de début_y":"debut_anesth", "Heure de fin_y":"fin_anesth", "Durée(minutes)_y":"duree_anesth"})
res_time = res_time.drop(columns=["Nom de l'événement_x", "Nom de l'événement_y"])
res_time = res_time.set_index("N°IPP")

res_recrutement = recrutement.groupby(by="N°IPP").count()
res_recrutement = res_recrutement[res_recrutement.columns[0]]