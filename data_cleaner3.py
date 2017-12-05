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
                  '^\\d[a-z]+\\d\\d[a-z][a-z]$':lambda s: s[0],
                  '^\\d[a-z]+$':lambda s: s[0],
                  '^\\d\\d[a-z]+$':lambda s: s[:1],
                  '^nr$':np.nan,
                  '^999.*$':np.nan, '^nonteste$':np.nan, '^oui':1, '^non':0,
                  '^si$':1, '^abdo$':1, '^[a-z]+lettredesortie$':np.nan,
                  '^acr$':1, '^att[a-z]+$':np.nan, '^(?![\s\S])':np.nan,
                  '^3ou4$':np.nan, '^babyshakingsynd$':1, '^externe$':1,
                  '^bou$':1, '^ctthoracique$':1, '^peutetre$':np.nan,
                  '^asthme$':1, '^admission24posttrauma$':1, '^ext$':np.nan,
                  '^.+vg$':lambda s: s[:-3], '^coag$':np.nan,
                  '^imprenable$':np.nan, '^inevaluable$':np.nan,
                  '^irregulier$':np.nan,'^regulier$':np.nan}

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
d = d[d.iss >= 16]  # drop those with iss < 16
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
d.quantiteprehosp.replace({10001500:1500, 7501000:1000, 7502000:2000, 8001000:1000},inplace=True)
d.grossessef.replace({'m':0, 'f':0, 2:1}, inplace=True)
d.poulsprehosp.replace({99111:np.nan}, inplace=True)
d.grossessef[d.sexe==1] = 1
d.cristalloidesperop.replace({'17624219sspi':np.nan},inplace=True)
d.cristalloidessu.replace({2000:1})
d.dropna(how='all', axis=0, inplace=True)
d.destinationalasortieduboxdusu.replace({'8sspiousoinsintermediaires2bl':8,
                                         '8sspisimpiousoinsintermediaires2bl':8,
                                         '8sspisimpiousoinsintermediaires2el':8},inplace=True)
d = d.rename(columns={'drainagethoraciqueouexsufflationprehosp':'drainagethoraciqueprehosp'})
d = d.rename(columns={'quantiteprehosp':'cristalprehosp'})
d = d.rename(columns={'quantiteprehosp_1':'colloprehosp'})
d = d.rename(columns={'quantiteprehosp1':'hyperosmprehosp'})
d = d.rename(columns={'dosagesu':'cristalsu'})
d = d.rename(columns={'dosagesu_1':'collosu'})
d = d.rename(columns={'dosagesu_2':'hyperosmsu'})
d = d.rename(columns={'totalculotserythrocytairesperopj0':'totceperopj0'})
d = d.rename(columns={'totalconcentreserythrocytairespdt1eres24heures':'totce24hsi'})

d['dtarriveedansleboxctthoracoabdo'] = pd.concat([d['dtarriveedansleboxctthoracoabdo'].dropna(), d['dtarriveedansleboxectthoracoabdo'].dropna()]).reindex_like(d)
d['heuredebutop2'] = pd.concat([d['heuredebutintervention1'].dropna(),
                                 d['heuredebutintervention_1'].dropna()]).reindex_like(d)
d['heuredebutop3'] = pd.concat([d['heuredebutintervention2'].dropna(),
                                 d['heuredebutintervention_2'].dropna()]).reindex_like(d)
d['heuredebutop4'] = pd.concat([d['heuredebutintervention3'].dropna(),
                                 d['heuredebutintervention_3'].dropna()]).reindex_like(d)
d['heuredebutop5'] = pd.concat([d['heuredebutintervention4'].dropna(),
                                 d['heuredebutintervention_4'].dropna()]).reindex_like(d)
d['heurefinop2'] = pd.concat([d['heurefindintervention1'].dropna(),
                                 d['heurefindintervention_1'].dropna()]).reindex_like(d)
d['heurefinop3'] = pd.concat([d['heurefindintervention2'].dropna(),
                                 d['heurefindintervention_2'].dropna()]).reindex_like(d)
d['heurefinop4'] = pd.concat([d['heurefindintervention3'].dropna(),
                                 d['heurefindintervention_3'].dropna()]).reindex_like(d)
d['heurefinop5'] = pd.concat([d['heurefindintervention4'].dropna(),
                                 d['heurefindintervention_4'].dropna()]).reindex_like(d)
d['jourpostopj01'] = pd.concat([d['jour'].dropna(),
                                 d['jourpostopj0'].dropna()]).reindex_like(d)
d['jourpostopj02'] = pd.concat([d['jour1'].dropna(),
                                 d['jourpostopj0_1'].dropna()]).reindex_like(d)
d['jourpostopj03'] = pd.concat([d['jour2'].dropna(),
                                 d['jourpostopj0_2'].dropna()]).reindex_like(d)
d['jourpostopj04'] = pd.concat([d['jour3'].dropna(),
                                 d['jourpostopj0_3'].dropna()]).reindex_like(d)
d['jourpostopj05'] = pd.concat([d['jour4'].dropna(),
                                 d['jourpostopj0_5'].dropna()]).reindex_like(d)
d['rehosp'] = pd.concat([d['rehospitalisationpourmemeevenement'].dropna(),
                        d['rehospitalisatonpourmemeevenement'].dropna()]).reindex_like(d)
d['comorbiditespreexistantes'] = pd.concat([d['comorbiditepreexistante'].dropna(),
                        d['comorbiditespreexistantes'].dropna()]).reindex_like(d)
d['cardiopathieischemiquesansinfarctusrecent'] = pd.concat([d['cardiopathieischemiquesansinfarctusrecent'].dropna(),
                        d['cardiopathie'].dropna()]).reindex_like(d)
d['isuffisancearterielle'] = pd.concat([d['isuffisancearterielle'].dropna(),
                        d['insuffisancearteriellepreexistante'].dropna()]).reindex_like(d)
d['pneumopathiechroniqueinsuffisancerespiratoire'] = pd.concat([d['pneumopathiechroniqueinsuffisancerespiratoire'].dropna(),
                        d['insuffisancerespiratoirepreexistante'].dropna()]).reindex_like(d)
d['hta'] = pd.concat([d['hta'].dropna(),
                        d['htapreexistante'].dropna()]).reindex_like(d)
d['insuffisancerenale'] = pd.concat([d['insuffisancerenale'].dropna(),
                        d['insuffisancerenalepreexistante'].dropna()]).reindex_like(d)
d['diabete'] = pd.concat([d['diabete'].dropna(),
                        d['diabetepreexistant'].dropna()]).reindex_like(d)
d['obesite'] = pd.concat([d['obesite'].dropna(),
                        d['obesitepreexistantebmi35obesitesevereoumorbide'].dropna()]).reindex_like(d)
d['maladiepsychiatriquedepressioninclus'] = pd.concat([d['maladiepsychiatriquedepressioninclus'].dropna(),
                        d['maladiepsychiatriquepreexistante'].dropna()]).reindex_like(d)
d['toxicomanieactive'] = d['toxicomanieactive'].combine_first(d['dependanceautresmedicaments']).reindex_like(d)
d['cirrhosehepatique'] = pd.concat([d['cirrhosehepatique'].dropna(),
                        d['cirrhosepreexistante'].dropna()]).reindex_like(d)
d['ohchronique'] = pd.concat([d['ohchronique'].dropna(),
                        d['ethylismechronique92013'].dropna()]).reindex_like(d)
d['tumeursolide'] = pd.concat([d['tumeursolide'].dropna(),
                        d['maladietumoraleactivepreexistante'].dropna()]).reindex_like(d)
d['tasprehosp'] = pd.concat([d['tasprehosp'].dropna(),
                        d['tasystoliqueprehosp'].dropna()]).reindex_like(d)
d['tadprehosp'] = pd.concat([d['tadprehosp'].dropna(),
                        d['tadiastoliqueprehosp'].dropna()]).reindex_like(d)
d['sato2prehosp'] = pd.concat([d['sato2prehosp'].dropna(),
                        d['sao2prehosp'].dropna()]).reindex_like(d)
d['gcstotalprehosp'] = pd.concat([d['gcstotalprehosp'].dropna(),
                        d['gcsprehosp'].dropna()]).reindex_like(d)
d['etco2co2expiresursiteprehosp'] = pd.concat([d['etco2co2expiresursiteprehosp'].dropna(),
                        d['etco2co2expprehosp'].dropna()]).reindex_like(d)
d['moyendetransportauxhug'] = pd.concat([d['moyendetransportauxhug'].dropna(),
                        d['moyendetransport'].dropna()]).reindex_like(d)
d['heuredarriveeadestination'] = d['heuredarriveeadestination'].combine_first(d['heuresursite']).reindex_like(d)
d['boxdedechocage'] = pd.concat([d['boxdedechocage'].dropna(),
                        d['boxerouge'].dropna()]).reindex_like(d)
d['gcstotalsu'] = pd.concat([d['gcstotalsu'].dropna(),
                        d['gcssu'].dropna()]).reindex_like(d)
d['tassu'] = pd.concat([d['tassu'].dropna(),
                        d['tasystoliquesu'].dropna()]).reindex_like(d)
d['tadsu'] = pd.concat([d['tadsu'].dropna(),
                        d['tadiastoliquesu'].dropna()]).reindex_like(d)
d['frsu'] = pd.concat([d['frsu'].dropna(),
                        d['frequencerespiratoiresu'].dropna()]).reindex_like(d)
d['evainitialesu'] = pd.concat([d['evainitialesu'].dropna(),
                        d['evaarriveesu'].dropna()]).reindex_like(d)
d['evafinpriseenchargesu'] = pd.concat([d['evafinpriseenchargesu'].dropna(),
                        d['evadepartsu'].dropna()]).reindex_like(d)
d['sato2su'] = pd.concat([d['sato2su'].dropna(),
                        d['saturationo2su'].dropna()]).reindex_like(d)
d['concentreserythrocytairessu'] = pd.concat([d['concentreserythrocytairessu'].dropna(),
                        d['culotserythrocytairessu'].dropna()]).reindex_like(d)
d['phsu'] = pd.concat([d['phsu'].dropna(),
                        d['phartsu'].dropna()]).reindex_like(d)
d['paco2su'] = pd.concat([d['paco2su'].dropna(),
                        d['paco2artsu'].dropna()]).reindex_like(d)
d['pao2su'] = pd.concat([d['pao2su'].dropna(),
                        d['pao2artsu'].dropna()]).reindex_like(d)
d['lactatesarterielsu'] = pd.concat([d['lactatesarterielsu'].dropna(),
                        d['lactateartsu'].dropna()]).reindex_like(d)
d['lactatesveineuxsu'] = pd.concat([d['lactatesveineuxsu'].dropna(),
                        d['lactateveineuxsu'].dropna()]).reindex_like(d)
d['hco3su'] = pd.concat([d['hco3su'].dropna(),
                        d['hco3artsu'].dropna()]).reindex_like(d)
d['hctsu'] = pd.concat([d['hctsu'].dropna(),
                        d['hctesu'].dropna()]).reindex_like(d)
d['rxthoraxsu'] = pd.concat([d['rxthoraxsu'].dropna(),
                        d['rxthorax'].dropna()]).reindex_like(d)
d['rxbassinsu'] = pd.concat([d['rxbassinsu'].dropna(),
                        d['rxbassin'].dropna()]).reindex_like(d)
d['echographiefastsu'] = pd.concat([d['echographiefastsu'].dropna(),
                        d['echographiefast'].dropna()]).reindex_like(d)
d['ctcerebralseulsu'] = pd.concat([d['ctcerebralseulsu'].dropna(),
                        d['ctcerebralseul'].dropna()]).reindex_like(d)
d['lieudela1ereinterventionaj0'] = pd.concat([d['lieudela1ereinterventionaj0'].dropna(),
                        d['lieudela1ereintervention'].dropna()]).reindex_like(d)
d['naturedela1ereainterventionj01'] = pd.concat([d['naturedela1ereainterventionj01'].dropna(),
                        d['naturedela1ereintervention'].dropna()]).reindex_like(d)
d['autresinterventionsaj02'] = pd.concat([d['autresinterventionsaj02'].dropna(),
                        d['autresinterventionsaj02emeint'].dropna()]).reindex_like(d)
d['tasperop'] = pd.concat([d['tasperop'].dropna(),
                        d['tasystoliqueperop'].dropna()]).reindex_like(d)
d['tadperop'] = pd.concat([d['tadperop'].dropna(),
                        d['tadiastoliqueperop'].dropna()]).reindex_like(d)
d['sato2perop'] = pd.concat([d['sato2perop'].dropna(),
                        d['saturationo2perop'].dropna()]).reindex_like(d)
d['temperatureperopt0'] = pd.concat([d['temperatureperopt0'].dropna(),
                        d['temperatureperop'].dropna()]).reindex_like(d)
d['phperop'] = pd.concat([d['phperop'].dropna(),
                        d['phartperop'].dropna()]).reindex_like(d)
d['lactatesperop'] = pd.concat([d['lactatesperop'].dropna(),
                        d['lactateartperop'].dropna()]).reindex_like(d)
d['baseexcessperop'] = pd.concat([d['baseexcessperop'].dropna(),
                        d['baseexcessartcbaseecfperop'].dropna()]).reindex_like(d)
d['novosevenperopenmg'] = pd.concat([d['novosevenperopenmg'].dropna(),
                        d['novosevenmgperop'].dropna()]).reindex_like(d)
d['cyclokapronperopenmg'] = pd.concat([d['cyclokapronperopenmg'].dropna(),
                        d['cyclocapronmgperop'].dropna()]).reindex_like(d)
d['fibrinogeneperopengr'] = pd.concat([d['fibrinogeneperopengr'].dropna(),
                        d['fibrinogenegrperop'].dropna()]).reindex_like(d)
d['anticoagulantsheparineperopenui'] = pd.concat([d['anticoagulantsheparineperopenui'].dropna(),
                        d['anticoagulantsheparineuiperop'].dropna()]).reindex_like(d)
d['heuredarriveeauxsij0'] = pd.concat([d['heuredarriveeauxsij0'].dropna(),
                        d['heuredarriveeauxsi'].dropna()]).reindex_like(d)
d['tassi'] = pd.concat([d['tassi'].dropna(),
                        d['tasystoliquesi'].dropna()]).reindex_like(d)
d['tadsi'] = pd.concat([d['tadsi'].dropna(),
                        d['tadiastoliquesi'].dropna()]).reindex_like(d)
d['sato2si'] = pd.concat([d['sato2si'].dropna(),
                        d['saturationo2si'].dropna()]).reindex_like(d)
d['nbcependant1eres24hsi'] = pd.concat([d['nbcependant1eres24hsi'].dropna(),
                        d['nbculotserythrocytairesj1si'].dropna()]).reindex_like(d)
d['nbpfcpendant1eres24hsi'] = pd.concat([d['nbpfcpendant1eres24hsi'].dropna(),
                        d['nbpfcj1si'].dropna()]).reindex_like(d)
d['nbthrombapheresespendant1eres24hsi'] = pd.concat([d['nbthrombapheresespendant1eres24hsi'].dropna(),
                        d['nbthrombapheresesj1si'].dropna()]).reindex_like(d)
d['dureeintubationsiheures'] = pd.concat([d['dureeintubationsiheures'].dropna(),
                        d['dureeintubationheuressi'].dropna()]).reindex_like(d)
d['totce24hsi'] = pd.concat([d['totce24hsi'].dropna(),
                        d['totalculotserythrocytairespdt1eres24heures'].dropna()]).reindex_like(d)
d['dureetotaleintubationsauxsiheures'] = pd.concat([d['dureetotaleintubationsauxsiheures'].dropna(),
                        d['dureeintubationheuressi'].dropna()]).reindex_like(d)
d['dureetotaledesejoursauxsiheures'] = d['dureetotaledesejoursauxsiheures'].combine_first(
                        d['dureedesejourauxsiheures']).reindex_like(d)
d['dureetotaledesejoursauxsijours'] = d['dureetotaledesejoursauxsijours'].combine_first(
                        d['dureedesejourauxsijours']).reindex_like(d)

#d.dropna(axis=0, inplace=True, thresh=((lambda x: round(x*0.055))(ds.shape[0])))  #0.55
#d.dropna(axis=1, inplace=True, thresh=((lambda x: round(x*0.4))(d.shape[1])))  #0.1
for cn,c in d.items():
    if c.unique().shape[0] <= 2 and c.isnull().values.any():
        d.drop([cn], axis=1, inplace=True)
d.columns = [s[:31] for s in d.columns]  #stata is limited to 32 chars variable length

# WRITE EXCEL FILE
epidemio = d[['nip', 'datedenaissance', 'datedelaccident', 'datedenaissance',
              'datedelaccident', 'age', 'sexe', 'pediatriejusqua16ans',
              'grossessef', 'poids', 'taille', 'bmi' ,'domicile',
              'typedepriseencharge', 'comorbiditespreexistantes',
              'cardiopathieischemiquesansinfar', 'infarctusmyocarde',
              'cardiopathieautre', 'isuffisancearterielle',
              'pneumopathiechroniqueinsuffisan', 'asthme', 'hta',
              'insuffisancerenale', 'diabete', 'obesite',
              'maladiepsychiatriquedepressioni', 'ohchronique',
              'cirrhosehepatique',
              'toxicomanieactive', 'tabagismeactif',
              'hemopathiemaligne', 'tumeursolide', 'immunosuppresseurs',
              'steroides', 'maladiecerebrovasculaire',
              'troublesdelacraseconstitutionne', 'troublesdelacraseacquis',
              'atcddemaladiethromboembolique', 'maladieneuromusculaire',
              'causedutrauma', 'mecanismedutrauma', 'typedetrauma',
              'alcoolisationaigueanamnese', 'alcoolemiemmoll', 'circonstancesaccident']]

prehosp = d[['absencededonnees', 'datedelalarme',
             'heuredelalarme', 'heurededepart', 'dtempsalarmedepart',
             'heuresursite', 'heurequittelieux', 'dtempsheuresursitequittelieux',
             'dureesursiteequipeprehosp20mn', 'heuredarriveeadestination',
             'lieudelaccident', 'zonedelaccident', 'decesavantlarriveedessecoursssr',
             'niveaudemedicalisationdessecour', 'interventionprimaire',
             'interventionsecondaire', 'naca', 'tasprehosp', 'tadprehosp',
             'poulsprehosp', 'frprehosp', 'evaprehosp', 'sato2prehosp',
             'fio2prehosp', 'gcsvprehosp', 'gcsmprehosp',
             'gcstotalprehosp', 'minerve', 'ceinturepelvienne',
             'drainagethoraciqueprehosp', 'intubationsursite', 'etco2co2expiresursiteprehosp',
             'etco2co2expireadestinationpreho', 'mceprehosp', 'sedationprehosp',
             'cristalloidesprehosp', 'cristalprehosp', 'colloidesprehosp',
             'colloprehosp', 'soluteshyperosmolairesprehosp', 'hyperosmprehosp',
             'aminesprehosp', 'mannitolprehosp', 'antalgieprehosp',
             'acidetranexaminqueprehosp', 'antibiotiquesprehosp', 
             'concentreserythrocytairesprehos', 'decessursite', 'moyendetransportauxhug',
             'sitransporthelico', 'hopitaltransfereurtransfertseco',
             'transportjusqualhopital1transfe', 'heurearriveedanshopital1',
             'heuredepartdelhopital1', 'motifdutransfert', 'exclusionseloncritereutstein']]

boxesu = d[['trisumotifcode', 'boxdedechocage', 'activationtraumateam',
            'heuredarriveedanslebox', 'dtempsalarmearriveedanslebox', 'gcsysu',
            'gcsvsu', 'gcsmsu', 'gcstotalsu', 'gcstotal9nonintubealarrivee',
            'poulssu', 'tassu', 'tadsu', 'temperaturesu', 'frsu', 'evainitialesu',
            'evafinpriseenchargesu', 'sato2su', 'fio2su', 'intubationsu', 'mcesu',
            'drainagethoraciquesu', 'interventiondansleboxsu',
            'interventiondansleboxsuautre', 'sedationsu', 'aminessu',
            'antibiotiquessu', 'cristalloidessu', 'cristalsu', 'colloidessu',
            'collosu', 'soluteshyperosmolairessu', 'hyperosmsu', 'mannitolsu',
            'antalgiesu', 'concentreserythrocytairessu', 'pfcsu', 'thrombapheresesu',
            'medicamentsprothrombotiquessu', 'cyclokapronmgsu',
            'fibrinogenegrsu', 'vitkmgkonakionsu', 'prothromplexuisu',
            'heuregazoartsu', 'heuregazoveinsu', 'phsu', 'paco2su', 'pao2su',
            'lactatesarterielsu', 'lactatesveineuxsu', 'hco3su', 'baseexcessartcbaseecfsu',
            'hbsu', 'hctsu', 'thrombocytessu', 'quicksu', 'inrsu', 'pttsu',
            'fibrinogenesu', 'rxthoraxsu', 'heurerxthorax', 'dtarriveedansleboxrxthorax',
            'rxbassinsu', 'heurerxbassin', 'dtarriveedansleboxrxbassin',
            'echographiefastsu', 'heureechographie', 'dtarriveedansleboxechographie',
            'ctcerebralseulsu', 'heurectcerebral', 'dtarriveedansleboxctcerebral',
            'ctthoracoabdoseul', 'heurectthoracoabdo', 'dtarriveedansleboxctthoracoabdo',
            'cttotalbodysu', 'heurecttotalbody', 'dtarriveedansleboxcttotalbody',
            'autrect', 'heureautrect', 'dtarriveedansleboxautrect',
            'extubationdansleboxsu', 'destinationalasortieduboxdusu',
            'decesdansleboxdusu', 'heurequittelieuxsu', 'dureedesejoursu']]

intervention = d[['interventionpdtsejour', 'datedela1ereintervention',
                  'intervention1aj0', 'lieudela1ereinterventionaj0', 
                  'heuredarriveeensalledinterventi', 'heuredebutdelanesthesie',
                  'heuredebutintervention', 'heuredebut1ereintervention',
                  'heurefindintervention', 'dureeintervention', 'departsalledintervention',
                  'firstkeyemergencyintervention1', 'firstkeyemergencyintervention2',
                  'firstkeyemergencyintervention3', 'firstkeyemergencyintervention4',
                  'naturedela1ereainterventionj01', 'autresinterventionsaj02',
                  'autresinterventionsaj03', 'autresinterventionsaj04',
                  'autresinterventionsaj05', 'poulsperop', 'tasperop', 'tadperop',
                  'sato2perop', 'temperatureperopt0', 'temperatureperopt30',
                  'feco2perop', 'acrperop', 'phperop', 'paco2artperop',
                  'pao2artperop', 'lactatesperop', 'baseexcessperop', 'hbperop',
                  'hcteperop', 'cristalloidesperop', 'colloidesperop', 'soluteshyperosmolairesperop',
                  'mannitolperop', 'aminesperop', 'totceperopj0', 'pfcperop',
                  'thrombaphereseperop', 'medicamentsprothrombotiquespero',
                  'novosevenperopenmg', 'cyclokapronperopenmg', 'fibrinogeneperopengr',
                  'vitkkonakionperopenmg', 'prothromplexuiperop', 'anticoagulantsheparineperopenui',
                  'destinationalasortiedubou',
                  #'intervention1j1', 'dateintervention1j1',
                  #'heuredebutop1', 'heurefinop1', 'jourpostopj01',  # not trivial, to review
                  'intervention2j1', 'dateintervention2j1',
                  'heuredebutop2', 'heurefinop2', 'jourpostopj02',
                  'intervention3j1', 'dateintervention3j1',
                  'heuredebutop3', 'heurefinop3', 'jourpostopj03',
                  'intervention4j1', 'dateintervention4j1',
                  'heuredebutop4', 'heurefinop4', 'jourpostopj04',
                  'intervention5j1', 'dateintervention5j1',
                  'heuredebutop5', 'heurefinop5', 'jourpostopj05',
                  'nombredepassageaubloc', 'acrperopj1', 'decesaubloc']]

soins_intensifs = d[['sejoursi', 'remarquessejourauxsi',
                     'heuredarriveeauxsij0', 'dtarriveeausuarriveeauxsi',
                     'tassi', 'tadsi', 'poulssi', 'temperaturesi', 'sato2si',
                     'intubesi', 'sedationsi', 'sousaminesa24hsi', 'antalgiea24hsi',
                     'nbcependant1eres24hsi', 'nbpfcpendant1eres24hsi',
                     'nbthrombapheresespendant1eres24', 'heuregazoartsi',
                     'heuregazoveinsi', 'phsi', 'lactatessi', 'hco3artsi',
                     'baseexcessartcbasebsi', 'baseexcessartcbaseecfsi',
                     'hbsi', 'hctesi', 'thrombocytessi', 'quicksi', 'inrsi',
                     'pttsi', 'fibrinogenesi', 'intubea24hsi', 'dureeintubationsiheures',
                     'dureedesejourauxsiheures', 'dureedesejourauxsijours',
                     'datesortiesi', 'scoresapsiisejour1', 'hemodialysehemofiltrationsi',
                     'mofsi', 'sepsissi', 'acrsi', 'ardssi', 'totce24hsi',
                     'totalpfcpdt1eres24heures', 'sejoursisejour2', 'dureeintubationsiheures_1',
                     'dureedesejourauxsijours_1', 'dureedesejourauxsiheures_1',
                     'datesortiesi_1', 'scoresapsiisejour2', 'sejoursisejour2',
                     'dureeintubationsiheures_2', 'dureedesejourauxsiheures_2',
                     'datesortiesi_2', 'scoresapsiisejour3', 'decessi',
                     'dureetotaleintubationsauxsiheur', 'dureetotaledesejoursauxsijours',
                     'dureetotaledesejoursauxsiheures']]

outcome = d[['datedesortie', 'dureedhospitalisationjours',
             'survivant', 'datedudeces', 'heuredudeces', 'djoursadmissiondeces',
             'lesioncerebralealasortie', 'destinationalasortiedelhopital',
             'suitedetraitement', 'complications', 'avc', 'infarctusmyocarde',
             'emboliepulmonaire', 'tvp', 'escarres', 'ir',
             'infectionplaie', 'pneumonie', 'infectionurinaire', 'sepsis',
             'syndromedeloges', 'ards', 'acr', 'mof',
             'autrespreciser', 'evolutionalongtermedeces28jours', 'rehosp'
             ]]

diagnostics_ais = d[sorted([i for i in d.columns.tolist() if re.search('headneck', i)]) +
                    sorted([i for i in d.columns.tolist() if re.search('face', i)]) +
                    sorted([i for i in d.columns.tolist() if re.search('chest', i)]) +
                    sorted([i for i in d.columns.tolist() if re.search('abdomen', i)]) +
                    sorted([i for i in d.columns.tolist() if re.search('pelvic', i)]) +
                    ['aisextremitiespelvis'] +
                    sorted([i for i in d.columns.tolist() if re.search('external', i)]) +
                    ['iss', 'niss', 'tarnps12', 'triss']]




writer = pd.ExcelWriter('./results/polytrauma.xlsx', engine='xlsxwriter')
epidemio.dropna(how='all').to_excel(writer, sheet_name='epidemio')
prehosp.dropna(how='all').to_excel(writer, sheet_name='prehosp')
boxesu.dropna(how='all').to_excel(writer, sheet_name='boxesu')
intervention.dropna(how='all').to_excel(writer, sheet_name='intervention')
soins_intensifs.dropna(how='all').to_excel(writer, sheet_name='soins_intensifs')
outcome.dropna(how='all').to_excel(writer, sheet_name='outcome')
diagnostics_ais.dropna(how='all').to_excel(writer, sheet_name='diagnostics_ais')
writer.save()

# FINAL DATASET
final_dataset = pd.concat([epidemio, prehosp, boxesu, intervention,
                           soins_intensifs, outcome, diagnostics_ais], axis=1)
final_dataset.to_csv('./results/polytrauma.csv')

#[i for i in d.columns.tolist() if re.search('accident', i)]

# WRITE FILE KEY
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


#[s.quantiteprehosp.unique() for s in sheets_sane.values() if 'quantiteprehosp' in s.columns]
#[cn+': '+ np.array_str(c.unique()) for cn,c in d.items() if len(c.unique().tolist())>2 and len(c.unique().tolist())<10]
#[cn+': '+ np.array_str(c.unique()) for cn,c in d.items() if c.dtype==np.dtype('O') and len(set([type(e) for e in c.unique().tolist()]))>2 and len(c.unique().tolist())<10]
#[cn+': '+ str([type(e) for e in c.unique().tolist()]) for cn,c in d.items() if c.dtype==np.dtype('O') and len(set([type(e) for e in c.unique().tolist()]))>2 and len(c.unique().tolist())<10]
