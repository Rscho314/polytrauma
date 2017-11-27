#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cleaning from unmodified raw sources directly to sql tables.
This file should be located in the same directory as the data files.
"""
from copy import deepcopy
import numpy as np
import os
import pandas as pd
import re
import sqlite3 as sql

#CONSTANTS
COMMON_INDEX = 'NIP'  # primary key for all indices
USELESS_SHEETS = ['Acceuil', 'Modifications', 'D-Epidémio', 'D-Préhosp',
                  'D-Box SU', 'D-Intervention', 'D-Soins intensifs',
                  'D-Diagnostics et Scores', 'Lettre info Patients', 'Feuil1',
                  'suivi modifications', 'Infos générales', 'Modifictions',
                  'D-Outcome']
# unable to find general criteria detect complex indices
COMPLEX_INDEX_BOUNDS = ['HEAD / NECK', 'FACE', 'CHEST', 'ABDOMEN',
                        'ETREMITIES / PELVIC GIRDLE', 'EXTERNAL']
# dictionary of forbidden characters in indices & their replacements
BAD_CHARS = {'Δ':'delta', ' ':'_', '/':'', '.':'', 'é':'e', 'è':'e', 'ï':'i',
             ':':'', '(':'', ')':'', 'à':'a', "'":'', '-':'', '≥':'>=',
             '+':''}
CLEAN_FILENAMES = {'Data_registre2013_20131231.xlsx':'2013',
                     'REGISTRE SUISSE 2016_version définitive.xlsx':'2016',
                     'REGISTRE SUISSE 2015_2017.03.09.xlsx':'2015',
                     'REGISTRE SUISSE 2017-2017-11-16.xlsx':'2017',
                     'REGISTRE SUISSE 2014_2015.03.10xlsx.xlsx':'2014'
                     }

# read excel files
files = [filename for filename in os.listdir() if filename.endswith('.xlsx')]
excel_files = {file:dict(pd.read_excel(file, None)) for file in files}

# sanitize file and sheet names, delete useless sheets (no mutation)
class renamer():
    """
    renames dataframe columns with suffix if duplicate of another column.
    """
    def __init__(self):
        self.d = dict()

    def __call__(self, x):
        if x not in self.d:
            self.d[x] = 0
            return x
        else:
            self.d[x] += 1
            return "%s_%d" % (x, self.d[x])

def sanitize_names(s):
    if pd.isnull(s):
        s = 'unknown'
    elif type(s) is not str:
        s = str(s)
    else:
        if s.endswith('_') or s.endswith(' '):  # this should be done above
            s = s[:-1]
        for bc,gc in BAD_CHARS.items():
            if bc in s:
                s = s.replace(bc, gc)
    s = re.sub('\_\_+', '_', s)  # probably also should go above
    return s.lower()

data = {CLEAN_FILENAMES[fn]:f for fn,f in excel_files.items()}
useful_data = {}
for fn,f in data.items():
    useful_data[fn] = {sanitize_names(sn):s for sn,s in f.items() if sn not in USELESS_SHEETS}

sheets = {fn + '_' + sn:s for fn,f in useful_data.items() for sn,s in f.items()}

# sanitize sheet indices (no mutation)
def sanitize_colnames(s):
    i = s.columns.tolist()
    if not incorrect_colnames(i) and not complex_colnames(i):
        ni = [sanitize_names(c) for c in i]
        ns = s.copy()
        ns.columns = ni
        return ns  #colnames is correct
    elif incorrect_colnames(i) and not complex_colnames(i):   # additional lines before colnames
        ni = s.iloc[find_colname_row(s),:]  # row containing COMMON_INDEX
        ns = s.copy()
        ns.columns = ni
        ns = ns[find_colname_row(ns) + 1:]  # remove rows above COMMON_INDEX
        if complex_colnames(ns.columns.tolist()):  # if complex after removing additional rows
            return simplify_colnames(ns)
        else:
            ns.columns = [sanitize_names(c) for c in ns.columns]
            return ns
    elif not incorrect_colnames(i) and complex_colnames(i):  # multiple rows colnames, of which only first row is considered
        ns = s.copy()
        return simplify_colnames(ns)
    # both incorrect_colnames(i) and complex_colnames(i) TRUE shouldn't happen

def incorrect_colnames(i):
    if COMMON_INDEX in i:
        return False
    else:
        return True

def complex_colnames(i):
    if set(i).intersection(set(COMPLEX_INDEX_BOUNDS)) != set():
        return True
    else:
        return False

def find_colname_row(s):
    row = np.where(s.astype(str) == COMMON_INDEX)  # str conversion bc exception if str/int mix
    return row[0][0]  # take first occurence of index

def simplify_colnames(s):
    """
    Flattens multiple rows colnames to a single row
    """
    colnames = s.columns.tolist()
    subcolnames = ['' if pd.isnull(e) else e for e in s.iloc[0,:].tolist()]
    start_bounds = [colnames.index(e) for e in COMPLEX_INDEX_BOUNDS]
    fragments = [colnames[bound + 1:] for bound in start_bounds]
    #next line raises StopIteration if fragment is not of the form [e, nan, ...]
    end_bounds = [next(f.index(e) for e in f if not pd.isnull(e)) for f in fragments]
    for e in list(zip(COMPLEX_INDEX_BOUNDS, start_bounds, end_bounds)):
        plug = [e[0] for i in range(e[2])]
        colnames[e[1] + 1:e[1] + 1 + e[2]] = plug
    newcolnames = ['' if pd.isnull(e) else e for e in colnames]
    simplified = ['{}_{}'.format(i, si) for i, si in list(zip(newcolnames, subcolnames))]
    ns = s[1:]
    ns.columns = [sanitize_names(c) for c in simplified]
    return ns

sheets_sane_index = {sn:sanitize_colnames(s).rename(columns=renamer()) for sn,s in sheets.items()}

# sanitize sheet contents

uniques = {}
for sn,s in sheets_sane_index.items():
    su = {}
    for c in s.columns:
        u = s[c].unique()
        su[c] = u
    uniques[sn] = su

for sn,s in uniques.items():
    for cn,c in s.items():
        if c.dtype == 'O':
            with open('./uniques/'+ sn + '_save', 'a') as f:
                f.write(sn + '_' + cn + ': ' + np.array_str(c) + '\n\n')
            
