# -*- coding: utf-8 -*-
"""
Cleaning from unmodified raw sources directly to usable dataset.
This file should be located in the same directory as the data files.
"""
from copy import deepcopy
import numpy as np
import os
import pandas as pd

files = [filename for filename in os.listdir() if filename.endswith('.xlsx')]
data = {file:pd.read_excel(file, None) for file in files}

#CONSTANTS
COMMON_INDEX = 'NIP'  # primary key for all indices
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
FORBIDDEN = {'Δ':'delta', ' ':'_', '/':'', '.':'', 'é':'e', 'è':'e', 'ï':'i',
             ':':'_', '(':'', ')':'', 'à':'a', "'":'', '-':'', '≥':'>='}

def find_sheet_index(sheet):
    index_loc = np.where(sheet.astype(str) == COMMON_INDEX)  # str conversion bc exception if str/int mix
    return index_loc[0][0]  # take first occurence of index

def has_complex_index(sheet):
    """
    Detects whether sheet has index on > 1 line.
    """
    index_row = find_sheet_index(sheet)
    if {e for e in sheet.iloc[index_row,:].tolist()}.intersection(COMPLEX_INDEX_BOUNDS) != set():
        return True
    else:
        return False
    
def simplify_complex_index(sheet, index_row):
    """
    Takes a 2 row index, fuses values to a single row, and deletes row 2.
    """
    index = sheet.iloc[index_row,:].tolist()
    subindex = ['' if pd.isnull(e) else e for e in sheet.iloc[index_row + 1,:].tolist()]
    start_bounds = [index.index(e) for e in COMPLEX_INDEX_BOUNDS]
    fragments = [index[bound + 1:] for bound in start_bounds]
    #next line raises StopIteration if fragment is not of the form [e, nan, ...]
    end_bounds = [next(f.index(e) for e in f if not pd.isnull(e)) for f in fragments]
    for e in list(zip(COMPLEX_INDEX_BOUNDS, start_bounds, end_bounds)):
        plug = [e[0] for i in range(e[2])]
        index[e[1] + 1:e[1] + 1 + e[2]] = plug
    index = ['' if pd.isnull(e) else e for e in index]
    simple_index = ['{}{}'.format(i, si) for i, si in list(zip(index, subindex))]
    
    return simple_index

def wrong_sheet_index(sheet):
    """
    Defines rules for validation of the sheet index, and returns whether ok.
    """
    if COMMON_INDEX in sheet.columns.tolist():  # test if index correct
        return False
    if ('Registre' in sheet.columns[0]) or (has_complex_index(sheet)):
        return True
    else:
        return False

def sanitize_index(index_aslist):
    """
    Iteratively modifies every column title to replace forbidden glyphs.
    Cannot do better than for loop due to str immutability.
    """
    l = []
    for v in index_aslist:
        a = v
        b = None
        for fc,fv in FORBIDDEN.items():
            b = a.replace(fc,fv)
            a = b
        l.append(a.lower())
    return l

def clean_sheet_index(sheet):
    """
    Defines rules to clean the sheet index if wrong_sheet_index() returns True.
    Does not mutate the sheet.
    """
    s = sheet.copy()  # avoid mutation
    index_row = find_sheet_index(s)
    new_index = s.iloc[index_row,:].tolist()
    if has_complex_index(s):
        new_index = simplify_complex_index(s, index_row)
        s = s[(index_row + 2):]
    else:
        s = s[(index_row + 1):]  # drop rows <= index
    new_index = ["unknown" if type(i) is not str else i for i in new_index]
    new_index = ["unknown" if i == '' else i for i in new_index]
    s.columns = new_index
    return s

def clean_data(d):
    """
    Groups all cleaning operations.
    Does not mutate original data, however files change OrderedDict -> Dict.
    """
    data = deepcopy(d)  # avoid mutation
    
    for f in list(data.values()):
        for k in list(f.keys()):
            # remove useless sheets
            if k in USELESS_SHEETS:
                del f[k]
            # additional rules as elif
            elif wrong_sheet_index(f[k]):
                f[k] = clean_sheet_index(f[k])
                f[k].columns = sanitize_index(f[k].columns)
            else:
                f[k].columns = sanitize_index([str(e) for e in f[k].columns])
    return data

def fuse_joined(col, data):
    """
    Fuses merged columns in dataframe.
    """
    print('yep')
    col = col.astype('object')
    col = col.fillna(data[col.name + SUFFIX[0]])  # priority given to left for fusion
    col = col.astype('object')
    col = col.fillna(data[col.name + SUFFIX[1]])
    #return col.name

def fuse_rows(row, names, common):
    rd = dict(zip(names, row))
    for c in common:
        rd[c] = rd[c + SUFFIX[0]]
        if pd.isnull(rd[c]):
            rd[c] = rd[c + SUFFIX[1]]
    return list(rd.values())

def make_dataset(d):
    """
    Performs an iterative outer join on all tables, to produce a single dataset
    """
    df = pd.DataFrame(data = {COMMON_INDEX.lower():[]})
    for fn,f in d.items():
        for sn, s in f.items():
            nd = pd.merge(df, s, how='outer', on=COMMON_INDEX.lower(),
                         suffixes=SUFFIX)

            cc = list(set(df.columns) & set(s.columns))  # common columns
            cc.remove(COMMON_INDEX.lower())
            cd = [c + SUFFIX[0] for c in cc] + [c + SUFFIX[1] for c in cc]
            #ct = cc + cd
            nd = nd.reindex(columns=[*nd.columns.tolist(), *cc])
            cr = list(set(nd.columns) - set(cd))
            nd = nd.astype('object')
            nd = nd.apply(lambda r,n,c: fuse_rows(r,n,c), args=(nd.columns,cc), axis=1)
            #nd[cc] = nd[cc].apply(lambda c,d: fuse_joined(c,d), args=(nd,), axis=0)
            nd = nd[cr]
            df = nd.copy()
    return df


clean = clean_data(data)
#dataset = make_dataset(clean)