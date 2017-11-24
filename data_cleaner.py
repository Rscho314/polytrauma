# -*- coding: utf-8 -*-
"""
Cleaning from unmodified raw sources directly to usable dataset
"""
from copy import deepcopy
import numpy as np
import os
import pandas as pd

files = [filename for filename in os.listdir() if filename.endswith('.xlsx')]
data = [pd.read_excel(file, None) for file in files]

COMMON_INDEX = 'NIP'

USELESS_SHEETS = ['Acceuil', 'Modifications', 'D-Epidémio', 'D-Préhosp',
                  'D-Box SU', 'D-Intervention', 'D-Soins intensifs',
                  'D-Diagnostics et Scores', 'Lettre info Patients', 'Feuil1',
                  'suivi modifications', 'Infos générales', 'Modifictions',
                  'D-Outcome']

def wrong_sheet_index(sheet):
    """
    Defines rules for validation of the sheet index, and returns whether ok.
    """
    if 'Registre' in sheet.columns[0]:
        return True
    else:
        return False

def clean_sheet_index(sheet):
    """
    Defines rules to clean the sheet index if wrong_sheet_index() returns True.
    Does not mutate the sheet.
    """
    s = sheet.copy()  # avoid mutation
    index_loc = np.where(s.astype(str) == COMMON_INDEX)  # str conversion bc exception if str/int mix
    index_row = index_loc[0][0]  # take first occurence of index
    new_index = s.iloc[index_row,:].tolist()
    new_index = ["unknown" if type(i) is not str else i for i in new_index]
    new_index = ["unknown" if i == '' else i for i in new_index]
    s = s[(index_row + 1):]  # drop rows <= index
    s.columns = new_index
    
    return s

def clean_data(d):
    """
    Groups all cleaning operations.
    Does not mutate original data, however files change OrderedDict -> Dict.
    """
    data = deepcopy(d)  # avoid mutation
    
    for f in data:
        for k in list(f.keys()):
            # remove useless sheets
            if k in USELESS_SHEETS:
                del f[k]
            # additional rules as elif
            elif wrong_sheet_index(f[k]):
                f[k] = clean_sheet_index(f[k])
    return data



[[k for k,v in file.items() if k not in USELESS_SHEETS] for file in data]