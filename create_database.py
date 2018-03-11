# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import glob
from sqlalchemy import create_engine
from dict_rename import *
from functools import reduce

def files_category(ff):
    '''
    List files based on substring
    '''
    return glob.glob('data/{}'.format(ff))

def make_anagrafica():
    '''
    Combine all 'anagarfica' files into one dataframe.
    Need to rename column names for simplicity
    '''
    tp = []
    for f in files_category('*SCUANA*'):

        anagr = pd.read_table(f, delimiter=',', encoding = "ISO-8859-1")

        #need to compute tag from file name
        if 'STAT' in f:
            tag = 'statale'
        else:
            tag = 'paritaria'
        anagr['tag'] = tag

        # standardise column name using custom dictionary
        new_cols = [dict_rename[c] if c in dict_rename.keys() else c for c in anagr.columns ]

        anagr.columns = [a.lower() for a in new_cols]
        tp.append(anagr)
    return pd.concat(tp, ignore_index = True)

def make_docenti():
    '''
    Return a combined multi-indexed version of 'docenti'
    '''
    doc = pd.read_table('data/DOCTIT20161720170831.csv', delimiter=',', encoding = "ISO-8859-1")
    sup = pd.read_table('data/DOCSUP20161720170831.csv', delimiter=',', encoding = "ISO-8859-1")
    doc.drop(columns = 'ANNOSCOLASTICO', inplace = True)
    sup.drop(columns = 'ANNOSCOLASTICO', inplace = True)

    doc.rename({'DOCENTITITOLARIMASCHI': 'M', 'DOCENTITITOLARIFEMMINE': 'F'}, axis = 1, inplace = True)
    sup.rename({'DOCENTISUPPLENTIMASCHI': 'M', 'DOCENTISUPPLENTIFEMMINE': 'F'}, axis = 1, inplace = True)

    # Exclude 'TIPOSUPPLENZA' by grouping and summing its vaules
    sup = sup.groupby(['PROVINCIA', 'ORDINESCUOLA', 'TIPOPOSTO', 'FASCIAETA']).sum().reset_index()

    doc['POSTO'] = 'DIRUOLO'
    sup['POSTO'] = 'SUPPLENZA'
    join_on = ['PROVINCIA', 'ORDINESCUOLA', 'TIPOPOSTO', 'FASCIAETA', 'POSTO', 'M', 'F']
    merged_df = pd.merge(doc, sup,  how='outer', on=join_on, sort=True)

    df = pd.pivot_table(merged_df,
                        values=['M','F'],
                        index=['PROVINCIA', 'ORDINESCUOLA'],
                        columns=['POSTO', 'TIPOPOSTO', 'FASCIAETA'])
    return df


def make_edilizia():
    '''
    Join all edilizia tables into one
    '''
    dfs = []
    for f in files_category('*EDI*'):
        edi = pd.read_table(f, delimiter=',', encoding = "ISO-8859-1")
        edi.columns = edi.columns.str.strip()
        dfs.append(edi)

    join_on = ['ANNOSCOLASTICO', 'CODICESCUOLA', 'CODICEEDIFICIO']
    df_final = reduce(lambda left,right: pd.merge(left,right,on=join_on), dfs)

    #Replace missing values
    missing = ['Informazione assente', '-']
    for m in missing:
        df_final.replace(m, np.nan, inplace = True)

    #Use only seismicity values S
    df_final.VINCOLIZONASISMICA = df_final.VINCOLIZONASISMICA.map(seismicity)

    new_cols = [dict_rename[c] if c in dict_rename.keys() else c for c in df_final.columns ]
    return df_final

dfs = []
for sub in ['CORSOETA', 'CORSOINDCLA','ITASTRACI','SECGRADOIND','TEMPOSCUOLA']:
    dfs = []
    for f in files_category('*{}*'.format(sub)):
        alu = pd.read_table(f, delimiter=',', encoding = "ISO-8859-1")
        alu.columns = alu.columns.str.strip()
        alu.rename({'ANNOCORSOCLASSE': 'ANNOCORSO'}, axis = 1, inplace = True)

        if 'STA' in f:
            tag = 'statale'
        else:
            tag = 'paritaria'
        alu['tag'] = tag

        dfs.append(alu)
    df_final = pd.concat(dfs, axis = 0)
    print(df_final.columns)

join_on = ['ANNOSCOLASTICO', 'CODICESCUOLA', 'ORDINESCUOLA', 'ANNOCORSO','tag']
df_final = reduce(lambda left,right: pd.merge(left,right,how = 'outer',on=join_on), dfs)
