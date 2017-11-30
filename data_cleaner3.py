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
from datetime import datetime,time
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
IMPROPER_CELLS = {'^\\d+$': lambda s: float(s),'^\\d*(inconnu|inconnue)$':np.nan,
                  '2perforant':2,
                  '2penetrant':3,
                  '^\\d[a-z]+$':lambda s: s[0],
                  '^\\d\\d[a-z]+$':lambda s: s[:1],
                  '^nr$':np.nan,
                  '^999.*$':np.nan, '^nonteste$':np.nan, '^oui':1, '^non':0,
                  '^si$':1, '^abdo$':1, '^[a-z]+lettredesortie$':np.nan,
                  '^acr$':1, '^att[a-z]+$':np.nan, '^(?![\s\S])':np.nan,
                  '^3ou4$':np.nan, '^babyshakingsynd$':1, '^externe$':1,
                  '^bou$':1, '^ctthoracique$':1, '^peutetre$':np.nan,
                  '^asthme$':1, '^admission24posttrauma$':1, '^ext$':np.nan}

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

def sanitize_contents(s):
    n = s.applymap(sanitize_with_regex)
    n = n.applymap(lambda x: np.nan if x == 999 else x)
    return n

#pd.DataFrame({'a':[1, 2, 3], 'b':['1a', 3, '3abc']})['b'].str.replace(re.compile('^\\d[a-z]$'), lambda x: x[0][0])

def sanitize_with_regex(s):
    if type(s) is not str:
        return s
    else:
        for k,v in IMPROPER_CELLS.items():
            if re.match(k, s):
                if not callable(v):
                    return v
                else:
                    return v(s)
        return s

#pd.DataFrame({'a':[1, 2, 3], 'b':['1a', 3, '3abc']}).applymap(sanitize_with_regex)

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

d = dataset
d.index = d.edsfid
d = sanitize_contents(d)
d = d[d.intervention1aj0.astype(float) == 1.0]  # drop those not having immediate surgery
l = ['datedenaissance', 'datedesortie', 'dateaccident',
     'datedelaccident', 'datedela1ereintervention']
t = ['dtarriveeausudebutpremiereintervention','dureedesejoursu',
     'dtempsalarmedepart','heuredebutdelanesthesie','heurededepart']
for c in l:
    d[c] = d[c].apply(lambda e: pd.to_datetime(e) if isinstance(e, float) else e)
for t1 in t:    
    d[t1] = d[t1].apply(lambda e: e.strftime('%H:%M:%S') if isinstance(e, (time, datetime)) else e)


d = d.drop(['edsfid','dossiernumerise','interventioncomplet','cp','autresperop',
            'prenom', 'nom', 'dossiercomplet', 'autres'], axis=1)
d.lieudelaccident.replace({'geneve':1, 'cantondegeneve':2, 'cantondevaud':3,
                           'extracantonal':4, 'france':5, 'fr':5},inplace=True)
d.destinationalasortiedelhopital.replace({16:6}, inplace=True)
d.mecanismedutrauma.replace({'troncdarbre':12},inplace=True)
d.domicile.replace({'geneve':1, 'france':3, 'autre':5, 'franceautre':4, 'autrecanton':2},inplace=True)
d.sexe.replace({'m':1, 'f':0}, inplace=True)
d.grossessef.replace({'m':0, 'f':0, 2:1}, inplace=True)
d.grossessef[d.sexe==1] = 1
d.cristalloidessu.replace({2000:1})
d.dropna(how='all', axis=0, inplace=True)
#d.dropna(axis=0, inplace=True, thresh=((lambda x: round(x*0.055))(ds.shape[0])))  #0.55
d.dropna(axis=1, inplace=True, thresh=((lambda x: round(x*0.5))(d.shape[1])))  #0.1
for cn,c in d.items():
    if c.unique().shape[0] <= 2 and c.isnull().values.any():
        d.drop([cn], axis=1, inplace=True)
d.to_csv('dataset.csv')

with open('key.txt', 'w') as f:
    f.write('niveaudemedicalisationdessecours'+':\n\t'+'\n\t'.join(['1 ambulanciers',
                                                      '2 cardiomobile',
                                                      '3 medecincadre',
                                                      '4 smurfr',
                                                      '5 aucun']))
    f.write('\n'+'destinationalasortiedelhopital'+':\n\t'+'\n\t'.join(['0 pathodcd',
                                                                       '1 domicile',
                                                                       '2 centredeconvalescence',
                                                                       '3 ems',
                                                                       '4 cliniquepsychiatrique',
                                                                       '5 rehabilitation',
                                                                       '6 autrehopital',
                                                                       '7 lieudedetention',
                                                                       '8autre']))
    f.write('\n'+'typedepriseencharge'+':\n\t'+'\n\t'.join(['1 transferthopitalperipherique',
                                                                       '2 transfertautretraumacenter',
                                                                       '3 admissionprimaire',
                                                                       '4 cmccentremedicochirurgical']))
    f.write('\n'+'lieudelaccident'+':\n\t'+'\n\t'.join(['1 geneve',
                                                        '2 cantondegeneve',
                                                        '3 cantondevaud',
                                                        '4 extracantonal',
                                                        '5 france',
                                                        '5 fr']))
    f.write('\n'+'mecanismedutrauma'+':\n\t'+'\n\t'.join(['1 avpoccupantvehiculeamoteur',
                                                          '2 avpmoto',
                                                          '3 avpvelo',
                                                          '4 avppieton',
                                                          '5 chutesahauteur',
                                                          '6 chutedesahauteur',
                                                          '8 autreaccidentdetrafictrainbateau',
                                                          '9struckby',
                                                          '10armeafeu',
                                                          '11 armeblanche',
                                                          '12autres',
                                                          '51chuteaski',
                                                          '21avalancheeboulement']))
    f.write('\n'+'domicile'+':\n\t'+'\n\t'.join(['1 geneve',
                                                 '2 autrecanton',
                                                 '3 france',
                                                 '4 franceautre',
                                                 '5 autre']))
    f.write('\n'+'typedetrauma'+':\n\t'+'\n\t'.join(['1 non penetrant non perforant',
                                                 '2 perforant',
                                                 '3 penetrant']))
    f.write('\n'+'destinationalasortieduboxdusu'+':\n\t'+'\n\t'.join(['1 bloc ou arterio',
                                                 '2 si',
                                                 '3 attente su',
                                                 '4 etage',
                                                 '5 pathodeces',
                                                 '6 transfertautrehopital',
                                                 '7 domicile',
                                                 '8 sspisimpiousoinsintermediaires2bl',
                                                 '10 autres']))
    f.write('\n'+'destinationalasortiedubou'+':\n\t'+'\n\t'.join(['1 si',
                                                 '2 sspi',
                                                 '4 boxurgenceuo',
                                                 '5 etage',
                                                 '6 deces']))
    f.write('\n'+'causedutrauma'+':\n\t'+'\n\t'.join(['1 accident',
                                                 '2 aggression',
                                                 '3 autoaggression']))
    
    

#[s.causedutrauma.unique() for s in sheets_sane.values() if 'causedutrauma' in s.columns]
#[cn+': '+ np.array_str(c.unique()) for cn,c in d.items() if len(c.unique().tolist())>2 and len(c.unique().tolist())<10]
#[cn+': '+ np.array_str(c.unique()) for cn,c in d.items() if c.dtype==np.dtype('O') and len(set([type(e) for e in c.unique().tolist()]))>2 and len(c.unique().tolist())<10]
#[cn+': '+ str([type(e) for e in c.unique().tolist()]) for cn,c in d.items() if c.dtype==np.dtype('O') and len(set([type(e) for e in c.unique().tolist()]))>2 and len(c.unique().tolist())<10]
