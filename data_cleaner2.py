#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cleaning from unmodified raw sources directly to sql tables.
This file should be located in the same directory as the data files.
"""
from copy import deepcopy
from datetime import datetime
import numpy as np
import os
import pandas as pd
import re
from unidecode import unidecode

#CONSTANTS
COMMON_INDEX = 'NIP'  # primary key for all indices
UNIQUE_INDEX = 'ui'  # unique index across all tables
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
BAD_CHARS = {'Δ':'delta', ' ':'_', '/':'', '.':'', 'é':'e', 'è':'e', 'ï':'i',
             ':':'', '(':'', ')':'', 'à':'a', "'":'', '-':'', '≥':'>=',
             '+':'', '\\':'', '\n':'', ',':'', 'ü':'u'}
CLEAN_FILENAMES = {'Data_registre2013_20131231.xlsx':'2013',
                     'REGISTRE SUISSE 2016_version définitive.xlsx':'2016',
                     'REGISTRE SUISSE 2015_2017.03.09.xlsx':'2015',
                     'REGISTRE SUISSE 2017-2017-11-16.xlsx':'2017',
                     'REGISTRE SUISSE 2014_2015.03.10xlsx.xlsx':'2014'
                     }

# read excel files
files = [filename for filename in os.listdir() if filename.endswith('.xlsx')]
excel_files = {file:dict(pd.read_excel(file, None)) for file in files}
# !!mutation correct misreading of 'Data_registre2013_20131231.xlsx/Diagnostics et scores 01.07.13'
excel_files['Data_registre2013_20131231.xlsx']['Diagnostics et scores 01.07.13'] = excel_files['Data_registre2013_20131231.xlsx']['Diagnostics et scores 01.07.13'][:116]

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
# getting unique values in each column
"""
uniques = {}
for sn,s in sheets_sane_index.items():
    su = {}
    for c in s.columns:
        u = s[c].unique()
        su[c] = u
    uniques[sn] = su
# write columns to files for easier visualization
for sn,s in uniques.items():
    for cn,c in s.items():
        if c.dtype == 'O':
            with open('./uniques/'+ sn + '_save', 'a') as f:
                f.write(sn + '_' + cn + ': ' + np.array_str(c) + '\n\n')
"""            
# join all tables to make the final dataset
def make_dataset_left(d, k):
    """
    Performs an iterative outer join on all tables, to produce a single dataset
    """
    df = pd.DataFrame({UNIQUE_INDEX:k})
    for sn,s in d.items():
        df = pd.merge(df, s, how='left', on=UNIQUE_INDEX, sort=False)
    return df

def make_dataset(d):
    '''
    Performs an iterative outer join on all tables, to produce a single dataset
    '''
    df = pd.DataFrame(columns=['ui'])
    for sn,s in d.items():
        nd = pd.merge(df, s, how='outer', on=UNIQUE_INDEX, suffixes=SUFFIX)
        cc = list(set(df.columns) & set(s.columns))  # common columns
        #cc.remove(UNIQUE_INDEX)
        cd = [c + SUFFIX[0] for c in cc] + [c + SUFFIX[1] for c in cc]
        cr = list(set(nd.columns) - set(cd))
        dfcc = pd.DataFrame(columns=cc)
        df = pd.merge(nd, dfcc, how='left', on=UNIQUE_INDEX, suffixes=SUFFIX)
        cc.remove(UNIQUE_INDEX)
        for c in cc:
            for s in SUFFIX:
                df[c] = df[c].fillna(df[c + s])
        df = df[cr]
    return df

# common columns to all tables
cc = None
for s in sheets_sane_index.values():
    if cc is not None:
       cc = set(s.columns).intersection(set(cc))
    else:
        cc = s.columns

# make a maximally unique index
sheets_unique_index = deepcopy(sheets_sane_index)
"""
ctrl = pd.DataFrame(columns=['sheet',
                     'unique_index?',
                     'any_null?'])
"""
for sn,s in sheets_unique_index.items():
    sheets_unique_index[sn] = s.dropna(subset=['edsfid'])
    sheets_unique_index[sn]['ui'] = sheets_unique_index[sn]['edsfid']
    #s['ui'] = s['edsfid'].apply(lambda x: ''.join(x.astype(str)))
    """
    ctrl = ctrl.append({'sheet':sn,
                        'unique_index?':bool(sheets_unique_index[sn].ui.unique().size<sheets_unique_index[sn].ui.size),
                        'any_null?':bool(sheets_unique_index[sn]['ui'].isnull().any())}, ignore_index=True)
print(ctrl)
"""
#keys = list(set(sum([s['ui'].tolist() for s in sheets_unique_index.values()],[])))
ds = make_dataset(sheets_unique_index)

# further clean the dataset (!!mutation)
# replace
ds.rename(columns={'ui':'eds'}, inplace=True)
ds = ds.set_index('eds')
ds.columns = [unidecode(c) for c in ds.columns]
ds = ds.applymap(lambda x: np.nan if type(x) is not str and x == 999 else x)
ds = ds.applymap(lambda s: unidecode(s) if type(s) is str else s)

# drop
"""
ds.drop(['edsfid'], axis=1, inplace=True)
ds.dropna(axis=0, inplace=True, thresh=((lambda x: round(x*0.055))(ds.shape[0])))  #0.55
ds.dropna(axis=1, inplace=True, thresh=((lambda x: round(x*0.1))(ds.shape[1])))  #0.1
ds.drop([c for c in ds.columns if 0 in ds[c].unique().tolist() and ds[c].unique().shape[0] is 2 and ds[c].isnull().values.any()],inplace=True,axis=1)
ds.drop([c for c in ds.columns if '0 = non' in ds[c].unique().tolist() and ds[c].unique().shape[0] is 2 and ds[c].isnull().values.any()],inplace=True,axis=1)
ds.drop(list(ds.filter(regex = 'unknown|unnamed|↓|^_\\d+$|^_*$|^[0-9]+$')),
        axis = 1, inplace = True)
"""
# replace
ds.replace(to_replace={'^0\s*=\s*non$':0, '^1\s*=\s*oui$':1,
                       '^\s*oui\s*$':1, '^\s*non\s*$':0, '^9\s*=\s*inconnu$':np.nan,
                       '^NR$':np.nan
                       },inplace=True,regex=True)
ds.grossesse_f.replace({'M':2, 'non':1, 'F':3},inplace=True)
ds.trauma_penetrant = ds.type_de_trauma.replace(to_replace={'1 = non penetrant':0,
                                      '1 = non-perforant':0,
                                      '2 = penetrant':1,
                                      '2 = perforant':0})
ds.trauma_perforant = ds.type_de_trauma.replace(to_replace={'1 = non penetrant':0,
                                      '1 = non-perforant':0,
                                      '2 = penetrant':0,
                                      '2 = perforant':1})
ds.gravite_trauma = ds.type_de_trauma.replace(to_replace={'1 = non penetrant':0,
                                      '1 = non-perforant':0,
                                      '2 = penetrant':1,
                                      '2 = perforant':1})
ds.destination_a_la_sortie_de_lhopital.replace({'0 = patho( DCD) ':0,
                                                '1 = domicile':1,
                                                '2 = centre de convalescence':2,
                                                '4 = clinique psychiatrique':4,
                                                '5 = rehabilitation':5,
                                                '6 = autre hopital':6,
                                                '16 = propre hopital/autre clinique':16}, inplace=True)
ds.sexe.replace({'F':0, 'M':1}, inplace=True)
ds.lieu_de_laccident.replace({'canton de Geneve':1,
                              'extra-cantonal':2,
                              'France':3,
                              'inconnu':np.nan}, inplace=True)
ds.domicile.replace({'Geneve':1,
                     'Autre canton':2,
                     'France':3,
                     'Autre':4},inplace=True)
ds.cause_du_trauma.replace({'1= accident':1,
                            '3= auto-agression':3,
                            '2= agression':2,
                            9:np.nan,
                            '3= auto-aggression':3},inplace=True)
ds.intervention_complet.replace({'v':1},inplace=True)
ds.mecanisme_du_trauma.replace({'4 = AVP pieton':4,
                                '1 = AVP occupant vehicule a moteur ':1,
                                '2 = AVP moto':3, '5 = Chute > sa hauteur':2,
                                '6 = Chute de sa hauteur':6,
                                '51 = chute a ski':51,
                                '999 = inconnu':np.nan,
                                '3 = AVP velo':3,
                                '11 = Arme blanche':11, '10 = Arme a feu':10,
                                '9 = struck by...':9,
                                '12 = Autres    ':12},inplace=True)
ds.duree_de_sejour_aux_si_jours.replace({datetime(1900, 1, 4, 0, 0):np.nan},inplace=True)
ds.destination_a_la_sortie_du_box_du_su.replace({'1a=bloc operatoire':1,
                                                 '2=SI':2,
                                                 '3=box SU ou UO':3,
                                                 '4=etage':4,
                                                 "1b=salle d'arterio":11},inplace=True)
ds.niveau_de_medicalisation_des_secours.replace({'3 = medecin cadre':3,
                                                 '2 = cardiomobile':2,
                                                 '1 = ambulanciers':1,
                                                 '999 = non renseigne':np.nan,
                                                 '4 = SMUR FR':4},inplace=True)
ds.intervention_dans_le_box_su.replace({"0 = pas d'intervention":1,
                                        '5 = pelviclamp':5,
                                        '6 = autre':6},inplace=True)
ds.exclusion_selon_critere_utstein.replace({'asphyxie':1},inplace=True)
ds.drainage_thoracique_ou_exsufflation_prehosp.replace({'1 = non':1,
                                                        '3 = drain thoracique':3},inplace=True)

ds.drop(['type_de_trauma', 'prenom','cp',
         'heure_gazo_vein_si', 'date_de_naissance'], axis=1, inplace=True)

# group by eds
#gds = ds.groupby('eds')
#dsf = gds.nth(0,'all')

ds.to_csv('final_dataset.csv')
print(ds.shape)