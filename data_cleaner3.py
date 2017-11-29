#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 28 03:40:31 2017

@author: raoul
"""

"""
Cleaning from unmodified raw sources directly to sql tables.
This file should be located in the same directory as the data files.
"""
from copy import deepcopy
from datetime import datetime
from functools import reduce
import numpy as np
import os
import pandas as pd
import re
from unidecode import unidecode

#CONSTANTS
INDEX = 'edsfid'  # unique index across all tables
SUFFIX = ('_x', '_y')  # _x & _y are the default suffix for pd.merge(). Modifiy as needed.
USELESS_SHEETS = ['Acceuil', 'Modifications', 'D-Epidémio', 'D-Préhosp',
                  'D-Box SU', 'D-Intervention', 'D-Soins intensifs',
                  'D-Diagnostics et Scores', 'Lettre info Patients', 'Feuil1',
                  'suivi modifications', 'Infos générales', 'Modifictions',
                  'D-Outcome']
# unable to find general criteria detect complex indices
COMPLEX_INDEX_BOUNDS = ['HEAD / NECK', 'FACE', 'CHEST', 'ABDOMEN',
                        'ETREMITIES / PELVIC GIRDLE', 'EXTERNAL']
# dictionary of forbidden characters in indices & their replacements

CLEAN_FILENAMES = {'Data_registre2013_20131231.xlsx':'2013',
                     'REGISTRE SUISSE 2016_version définitive.xlsx':'2016',
                     'REGISTRE SUISSE 2015_2017.03.09.xlsx':'2015',
                     'REGISTRE SUISSE 2017-2017-11-16.xlsx':'2017',
                     'REGISTRE SUISSE 2014_2015.03.10xlsx.xlsx':'2014'
                     }
# regex patterns for columns
DROPPED_COLUMNS = '^unnamed|^[0-9]+$|^[0-9]+\\.[0-9]+$|^\\.[0-9]+$'

#regex patterns for cells
IMPROPER_CELLS = ''

# read excel files
files = [filename for filename in os.listdir() if filename.endswith('.xlsx')]
excel_files = {file:dict(pd.read_excel(file, None)) for file in files}
# !!mutation correct misreading of 'Data_registre2013_20131231.xlsx/Diagnostics et scores 01.07.13'
excel_files['Data_registre2013_20131231.xlsx']['Diagnostics et scores 01.07.13'] = excel_files['Data_registre2013_20131231.xlsx']['Diagnostics et scores 01.07.13'][:116]

# sanitize file and sheet names, delete useless sheets (no mutation)
class duplicate_columns_renamer():
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

def sanitize(s):
    try:
        return unidecode(''.join(c for c in s if c.isalnum())).lower()
    except TypeError:
        return s

data = {CLEAN_FILENAMES[fn]:f for fn,f in excel_files.items()}
USELESS_SHEETS = [sanitize(s) for s in USELESS_SHEETS]
COMPLEX_INDEX_BOUNDS = [sanitize(s) for s in COMPLEX_INDEX_BOUNDS]
useful_data = {}
for fn,f in data.items():
    useful_data[fn] = {sanitize(sn):s.applymap(lambda e: sanitize(e)) for sn,s in f.items() if sanitize(sn) not in USELESS_SHEETS}

sheets = {fn + sn:s for fn,f in useful_data.items() for sn,s in f.items()}


# sanitize sheet indices (no mutation)
def sanitize_index_structure(s):
    i = [sanitize(c) for c in s.columns]
    if not incorrect_colnames(i) and not complex_colnames(i):
        ns = s.copy()
        ns.columns = i
        return ns  #colnames is correct
    elif incorrect_colnames(i) and not complex_colnames(i):   # additional lines before colnames
        ni = s.iloc[find_colname_row(s),:]  # row containing INDEX
        ns = s.copy()
        ns.columns = ni
        ns = ns[find_colname_row(ns) + 1:]  # remove rows above INDEX
        if complex_colnames(ns.columns.tolist()):  # if complex after removing additional rows
            return simplify_colnames(ns)
        else:
            return ns
    elif not incorrect_colnames(i) and complex_colnames(i):  # multiple rows colnames, of which only first row is considered
        ns = s.copy()
        ns.columns = i
        return simplify_colnames(ns)
    # both incorrect_colnames(i) and complex_colnames(i) TRUE shouldn't happen

def incorrect_colnames(i):
    if INDEX in i:
        return False
    else:
        return True

def complex_colnames(i):
    if set(i).intersection(set(COMPLEX_INDEX_BOUNDS)) != set():
        return True
    else:
        return False

def find_colname_row(s):
    row = np.where(s.astype(str) == INDEX)  # str conversion bc exception if str/int mix
    return row[0][0]

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
    simplified = ['{}{}'.format(i, si) for i, si in list(zip(newcolnames, subcolnames))]
    ns = s[1:]
    ns.columns = simplified
    return ns

def remove_null_columns(s):  # no mutation
    n = s.copy()
    n.columns = [str(col) for col in n.columns]
    n = n.drop([c for c in n.columns if c in ['nan', 'NaT','']],axis=1)
    n = n.dropna(axis=1, how='all')
    return n

def remove_columns_regex(s):
    n = s.drop(list(s.filter(regex = DROPPED_COLUMNS)),axis = 1)
    return n

def sanitize_rows(s):
    n = s.dropna(subset=[INDEX], axis=0)
    # remove duplicate edsfid, cause not possible to get unique index across all sheets (only 2-3 observations)
    n = n.drop_duplicates(subset=[INDEX])
    return n

def sanitize_all(s):
    n = sanitize_index_structure(s)
    n = remove_null_columns(n)
    n = n.rename(columns= duplicate_columns_renamer())
    n = n.rename(columns=lambda c: c.lstrip('0123456789'))
    n = remove_columns_regex(n)
    n = sanitize_rows(n)
    return n

sheets_sane = {sn:sanitize_all(s) for sn,s in sheets.items()}

# make dataset
def outer_join(l, r):
    nd = pd.merge(l, r, how='outer', on=INDEX, suffixes=SUFFIX)
    cc = list(set(l.columns) & set(r.columns))  # common columns
    cc.remove(INDEX)
    nd = pd.concat([nd ,pd.DataFrame(columns=cc)])
    cd = [c + SUFFIX[0] for c in cc] + [c + SUFFIX[1] for c in cc]
    cr = list(set(nd.columns) - set(cd))
    for c in cc:
        for s in SUFFIX:
            nd[c] = nd[c].fillna(nd[c + s])
    nd = nd[cr].groupby([INDEX]).first().reset_index()
    #if len(set(nd.edsfid)) != len(set(l.edsfid).union(set(r.edsfid))):
    return nd

dataset = reduce(lambda l,r: outer_join(l,r), sheets_sane.values())

dataset.to_csv('dataset.csv')
