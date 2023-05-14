import pandas as pd
import numpy as np
from pydantic import BaseModel, Field
from typing import Optional

### Create Class of reference
# class Table(BaseModel):
#     tablename: str
#     tableschema: dict

# bu = Table(tablename='ya_tu_sabe_la_tabla', tableschema={'col_1': int, 'col_2': str})
# print(bu)



### Read data only
# dep_filename = "departments.csv"
# df_dep = pd.read_csv(dep_filename, names=["id", "department"], sep=',', skiprows=0, nrows=300)
# print(df_dep.info())
# for index, row in df_dep.iterrows():
#     print(row['id'], row['name'])



### Read data and handling nulls
dep_filename = "departments.csv"
sch = {'id': 'int', 'department': 'str'}
df_dep = pd.read_csv(dep_filename, names=["id", "department"], dtype=sch, sep=',', skiprows=0, nrows=300)
# df_dep = pd.read_csv(dep_filename, names=["id", "name"], sep=',', skiprows=0, nrows=300)
print(df_dep.dtypes)
df_dep = df_dep.fillna('NULL')
null_ph_int = -999
null_ph_str = 'NULL'
dc_nulls = dict()
for key, value in sch.items():
    if value == 'int':
        dc_nulls[key] = null_ph_int
    else:
        dc_nulls[key] = null_ph_str
print(dc_nulls)



### Proof to consider only the allowed indexes

# ar_ids = set(list(df_dep["id"].values))
# print(type(ar_ids))
# print(ar_ids)

# ids_pred = [3,6,7,15,16]
# set_ids_pred =set(ids_pred)

# allowindexes = list(ar_ids.difference(ids_pred))
# df_dep_allowed = df_dep[df_dep["id"].isin(allowindexes)]
# print(df_dep_allowed.head(20))
# print(ar_ids.intersection(ids_pred))



### Proof needed to insert NULL values in a tuple

# list_data = [list(x) for x in df_dep.to_numpy()]
# [tuple([None if c=='NULL' else c for c in elem]) for elem in list_data]
#     print(elem)
#     elem = tuple([None if c=='NULL' else c for c in elem])
#     print(elem)
