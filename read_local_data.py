import pandas as pd

dep_filename = "departments.csv"
df_dep = pd.read_csv(dep_filename, names=['id','name'], sep=',')
print(df_dep.info())

for index, row in df_dep.iterrows():
    print(row['id'], row['name'])