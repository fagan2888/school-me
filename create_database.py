# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import glob
from sqlalchemy import create_engine
from dict_rename import *

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
        new_cols = []
        for c in anagr.columns:
            new_c = c
            if c in rename_anagrafica:
                new_c = rename_anagrafica[c]
            new_cols.append(new_c)

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
