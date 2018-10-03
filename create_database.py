# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import glob
from dict_rename import *
from functools import reduce
from sqlalchemy import create_engine

class MakeData:
    def __init__(self):
        self.con = create_engine('sqlite:///IT_schools_1819.db', echo=True)

    @staticmethod
    def files_category(ff):
        '''
        List files based on substring
        '''
        return glob.glob('data/{}'.format(ff))

    @staticmethod
    def stat_par(f):
        if 'STA' in f:
            tag = 'statale'
        else:
            tag = 'paritaria'
        return tag

    def make_anagrafica(self):
        '''
        Combine all 'anagarfica' files into one dataframe.
        Need to rename column names for simplicity
        '''
        tp = []
        for f in self.files_category('*SCUANA*'):

            anagr = pd.read_table(f, delimiter=',', encoding = "ISO-8859-1")

            #need to compute tag from file name
            anagr['tag'] = self.stat_par(f)

            # standardise column name using custom dictionary
            new_cols = [dict_rename[c] if c in dict_rename.keys() else c for c in anagr.columns ]

            anagr.columns = [a.lower() for a in new_cols]
            tp.append(anagr)

        return pd.concat(tp, ignore_index = True)

    def make_docenti(self):
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

        # df = pd.pivot_table(merged_df,
        #                     values=['M','F'],
        #                     index=['PROVINCIA', 'ORDINESCUOLA'],
        #                     columns=['POSTO', 'TIPOPOSTO', 'FASCIAETA'])
        return merged_df

    def make_edilizia(self):
        '''
        Join all edilizia tables into one
        '''
        dfs = []
        for f in self.files_category('*EDI*'):
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

    def make_students(self):
        '''
        Return two dataframes
           - corso_eta contains students per age cohorts
           - corso_tot contains total students per class with nationality info
        '''
        # Not using TEMPOSCUOLA and SECGRADOIND
        df_final = []
        for sub in ['CORSOETA', 'CORSOINDCLA','ITASTRACI']:
            dfs = []
            for f in self.files_category('*{}*'.format(sub)):
                alu = pd.read_table(f, delimiter=',', encoding = "ISO-8859-1")
                alu.columns = alu.columns.str.strip()
                #rename ANNOCORSOCLASSE for consistency with other tables
                alu.rename({'ANNOCORSOCLASSE': 'ANNOCORSO'}, axis = 1, inplace = True)
                alu['tag'] = self.stat_par(f)
                dfs.append(alu)
            dfcomb = pd.concat(dfs, axis = 0)
            df_final.append(dfcomb)

        corso_eta = df_final[0]

        join_on = ['ANNOSCOLASTICO', 'CODICESCUOLA', 'ORDINESCUOLA', 'ANNOCORSO', 'tag']
        corso_tot = df_final[2]
        new_cols = [dict_rename[c] if c in dict_rename.keys() else c for c in corso_tot.columns ]
        corso_tot.columns = new_cols

        return corso_eta, corso_tot

    def make_demographic(self):
        '''
        return dataframe with population per city
        '''
        demog = pd.read_excel('data/Elenco-codici-statistici-e-denominazioni-al-01_01_2017.xls',
                               dtype = {'Codice Comune formato alfanumerico': str})
        mask = demog['Denominazione provincia'] == '-'
        demog.loc[mask,'Denominazione provincia'] = demog.loc[mask, 'Denominazione Città metropolitana']

        mask = demog.columns.isin(doc_comuni.keys())
        cols = demog.columns[mask]

        demog = demog[cols].rename(columns = doc_comuni)
        return demog

    def make_valutazione(self):
        '''
        Return a combined multi-indexed version of 'docenti'
        '''
        tp = []
        for f in self.files_category('*VALUTAZIONE*'):

            val = pd.read_table(f, delimiter=',', encoding = "ISO-8859-1")

            #need to compute tag from file name
            val['tag'] = self.stat_par(f)

            # standardise column name using custom dictionary
            new_cols = [dict_rename[c] if c in dict_rename.keys() else c for c in val.columns ]

            val.columns = [a.lower() for a in new_cols]
            tp.append(val)

        return pd.concat(tp, ignore_index = True)

    def make_database(self):
        self.make_anagrafica().to_sql('anagrafica', con = self.con, if_exists = 'replace', index = False)
        self.make_docenti().to_sql('docenti', con = self.con, if_exists = 'replace', index = False)
        self.make_edilizia().to_sql('edilizia', con = self.con, if_exists = 'replace', index = False)
        self.make_students()[0].to_sql('studenti_eta', con = self.con, if_exists = 'replace', index = False)
        self.make_students()[1].to_sql('studenti_aggr', con = self.con, if_exists = 'replace', index = False)
        self.make_demographic().to_sql('demografica', con = self.con, if_exists = 'replace', index = False)
        self.make_valutazione().to_sql('valutazione', con = self.con, if_exists = 'replace', index = False)

if __name__ == '__main__':
    MakeData().make_database()
