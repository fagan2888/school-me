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
        return pd.concat(tp, ignore_index = True)
