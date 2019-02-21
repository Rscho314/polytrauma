#!/usr/bin/env python3

import os
import numpy as np
import pandas as pd
from functools import reduce

# Read DANI data
print("reading files names...")
p = "/home/raoul/Desktop/polytrauma/DANI"
ps = [os.path.join(p,fn) for fn in os.listdir(p) if fn.endswith('.xlsx')]


# Get data into pandas
print("creating Pandas datasets...")
pandas_data = {os.path.split(name)[1] : (pd.read_excel(name,
                                                       header=0,
                                                       skiprows=6,
                                                       usecols=4,
                                                       index_col=0)
                                        if os.path.split(name)[1] not in ["polytrauma.xlsx"]
                                        else pd.read_excel(name, index_col=1)) for name in ps}

pandas_data['ventilation2.xlsx'] = pd.read_excel(os.path.join(p,
                                                              'ventilation2.xlsx'),
                                                 header=0,
                                                 usecols=4,
                                                 index_col=0)

# separate raw DANI files from custom file
# remove 1st row
dani_data = {name:data.drop(data.index[0]) for (name, data) in pandas_data.items() if name not in ['polytrauma.xlsx', 'ventilation2.xlsx']}
dani_data['ventilation2.xlsx'] = pandas_data['ventilation2.xlsx']

# base datasets
eds_2013_2017 = pd.read_csv(os.path.join(p,"eds_2013-2017.csv"))  # liste 284 patients par M Licker
eds_2018 = pd.read_csv(os.path.join(p,"eds_2018.csv"))
eds_all = pd.concat([eds_2013_2017, eds_2018])
custom = pandas_data["polytrauma.xlsx"]
time_sample = pd.DataFrame(index=custom.index, data={"period":pd.PeriodIndex(custom.datedela1ereintervention, freq="2D")})

def clean_before_agg(d):
    d.index.name = "IPP"
    d.index = d.index.astype(np.int64)
    if "Heure de début" in d.columns:
        d1 = d.rename(columns={"Heure de début":"reftime"})
    if "Heure" in d.columns:
        d1 = d.rename(columns={"Heure":"reftime"})
    d2 = pd.concat([d1, time_sample], axis=1, join="inner")
    return d2[(d2.period.dt.start_time < d2.reftime) & (d2.period.dt.end_time > d2.reftime)]


events = [dani_data["polytrauma_duree.xlsx"], dani_data["polytrauma_duree_2018.xlsx"]]
events = pd.concat(events)
events = clean_before_agg(events)
time = events[events["Nom de l'événement"]!="Recrutement pulmonaire"]
recrutement = events[events["Nom de l'événement"]=="Recrutement pulmonaire"]

rest = {name: data for (name, data) in dani_data.items() if name not in ["polytrauma_duree.xlsx", "polytrauma_duree_2018.xlsx"]}
signal = pd.concat(rest.values())
signal = clean_before_agg(signal)

# intermediate datasets
duration = time.groupby([time.index, "Nom de l'événement"]).head(1)
intervention = duration[duration["Nom de l'événement"]=="Intervention"]
anesthesie = duration[duration["Nom de l'événement"]=="Anesthésie"]
signals = {k:v for k,v in signal.groupby(by="Nom du paramètre")}
signals_grouped = {name:data.groupby(by=data.index) for name,data in signals.items()}
varnames = ['freq_resp', 'freq_card', 'freq_cardp',
            'hct', 'hb', 'lactate', 'ph', 'peep',
            'ktart_dia', 'ktart_moy', 'ktart_sys',
            'pni_dia', 'pni_moy', 'pni_sys',
            'p_plateau', 'urines',
            'tidal_exp', 'tidal_ins', 'tidal_set', 'vent_min_exp', 'vent_min_spont']
signals_new_names = {nk:signals_grouped[k] for (k, nk) in zip(list(signals_grouped.keys()), varnames)}

def perc25(x):
    return np.percentile(x, 25)

def perc75(x):
    return np.percentile(x, 75)

signals_stats = {name:data.agg([np.mean, np.std, np.median, perc25, perc75, np.min, np.max, np.sum]).rename(columns={"mean":name+"_"+"mean", "std":name+"_"+"std", "median":name+"_"+"median", "perc25":name+"_"+"perc25", "perc75":name+"_"+"perc75", "amin":name+"_"+"min", "amax":name+"_"+"max", "sum":name+"_"+"sum"}) for (name,data) in signals_new_names.items()}
for v in signals_stats.values():
    v.columns = v.columns.droplevel(0)

#results
res_custom = custom.rename(columns={"Debut anesthhésie":"debut_anesth", "Fin anesthésie":"fin_anesth", "Durée aanesthésie":"duree_anesth", "Début intervention":"debut_chir", "Fin iintetrvention":"fin_chir", "Durée interventtiton":"duree_chir"})

res_time = pd.merge(intervention, anesthesie, left_index=True, right_index=True)
res_time = res_time.rename(columns={"reftime_x":"debut_chir", "Heure de fin_x":"fin_chir", "Durée(minutes)_x":"duree_chir","reftime_y":"debut_anesth", "Heure de fin_y":"fin_anesth", "Durée(minutes)_y":"duree_anesth"})
res_time = res_time.drop(columns=["Nom de l'événement_x", "Nom de l'événement_y", "period_x", "period_y"])
res_time_custom = pd.merge(res_custom, res_time, left_index=True, right_index=True, how="outer", validate="one_to_one")

res_recrutement = recrutement.groupby(by=recrutement.index).count()
res_recrutement = res_recrutement[res_recrutement.columns[0]]

res_signal = reduce(lambda a,b: pd.concat([a, b], axis=1, join="outer"), signals_stats.values(), pd.DataFrame(data={"eds":res_custom.edsfid}, index=res_custom.index))

# final result
print("creating final dataset...")
final = pd.DataFrame(index=res_custom.index, columns=list(res_time.columns)+["recrutement"]+list(res_signal.columns))

for c in list(final.columns):
    if c in list(res_custom.columns):
        final[c] = final[c].fillna(res_custom[c])
    if c in list(res_time.columns):
        final[c] = final[c].fillna(res_time[c])
    if c in list(res_signal.columns):
        final[c] = final[c].fillna(res_signal[c])
    if c == "recrutement":
        final["recrutement"] = final["recrutement"].fillna(res_recrutement)
    if c in ["debut_chir", "fin_chir", "debut_anesth", "fin_anesth"]:
        final[c] = pd.DatetimeIndex(final[c])

# filter patients not in M Licker's list for 2013-2017
final = final[final["eds"].isin(eds_all["eds"])]


# Write final files
final.to_excel("../final_polytrauma.xlsx")
writer = pd.io.stata.StataWriter('../final_polytrauma.dta', final)
writer.write_file()
print("done.")
