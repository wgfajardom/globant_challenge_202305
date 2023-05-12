# app_globant.py

### Import libraries
from fastapi import FastAPI
from pydantic import BaseModel, Field
import pandas as pd
import psycopg2
import sys


### Initialize API
app_globant = FastAPI()


### Database connection
CONNECT_DB = "host=localhost port=5432 dbname=globant user=postgres password=GlobantChallenge"
try:
    # Make connection to db
    cxn = psycopg2.connect(CONNECT_DB)    
    # Create a cursor to db
    cur = cxn.cursor()
except (Exception, psycopg2.Error) as error:
    print("Error while connecting to PostgreSQL", error)
    sys.exit()


### Definition of Classes

# Class File
class File(BaseModel):
    filename: str

# Class File
class Table(BaseModel):
    tablename: str

# Class Department
class Department(BaseModel):
    department_id: int = Field(alias="id")
    name: str




### HTTP methods for departments

# Retriving data from database

def select_table(cur, table):
    cur.execute("SELECT * FROM {};".format(table))
    raw_ls = list(cur.fetchall())
    if len(raw_ls) > 0:
        output_ls = [Department(id=elem[0], name=elem[1]) for elem in raw_ls]
    else:
        output_ls = []
    return output_ls

# # Load departments using the class Deparment
# departments=[]
# for index, row in df_dep.iterrows():
#     aux_dep = Department(id=row['id'], name=row['name'])
#     departments.append(aux_dep)

# GET request for retrieving the list of departments
@app_globant.get("/departments")
async def get_departments():
    return select_table(cur, 'departments')

# POST request to load data into the database
@app_globant.post("/departments", status_code=201)
async def add_deparments(file: File, table: Table):

    # Read file and copy to database
    filename = file.filename
    tablename = table.tablename
    with open(filename, 'r') as f:
        cur.copy_from(f, tablename, sep=",")
        cxn.commit()

    return select_table(cur, 'departments')


# # Closing database connection.
# if(cxn):
#     cur.close()
#     cxn.close()
# print("PostgreSQL connection is closed")
