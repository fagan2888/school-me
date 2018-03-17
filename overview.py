from create_database import *
import numpy as np

ana = make_anagrafica()
doc = make_docenti()
edi = make_edilizia()
stu_eta, stu_agg = make_students()
demog = make_demographic()

active_schools = ana[ana['codice_scuola'].isin(stu_agg.codice_scuola.unique())]
active_schools.set_index('codice_scuola', inplace = True, drop = True)

students_per_school = stu_agg.groupby('codice_scuola').sum()
students_per_school = pd.concat([active_schools, students_per_school], axis = 1)
cols = ['ALUNNI', 'ITALIAN','NON_ITALIAN', 'EU', 'NON_EU']
students_grouped = students_per_school.groupby(['regione']).sum()[cols]

to_append = students_grouped.sum()
to_append.name = 'TOT'
students_grouped = students_grouped.append(to_append)

students_grouped['foreign_percentage'] = np.round(students_grouped['NON_ITALIAN']/students_grouped['ITALIAN'] * 100,1)
students_grouped['foreign_eu_percentage'] = np.round(students_grouped['EU']/students_grouped['NON_ITALIAN'] * 100, 1)
students_grouped['foreign_noeu_percentage'] = np.round(students_grouped['NON_EU']/students_grouped['NON_ITALIAN'] * 100, 1)

students_grouped.sort_values('ALUNNI')
