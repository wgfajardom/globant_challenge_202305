# app_globant.py

### Import libraries
from fastapi import FastAPI
# from fastapi import Body
from pydantic import BaseModel, Field
from typing import Optional
# from typing import Annotated
import pandas as pd
# import numpy as np
import psycopg2
import psycopg2.extras as extras
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
    sta_ind: Optional[int] = 1
    end_ind: Optional[int] = 1000

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


# GET request for retrieving the list of departments
@app_globant.get("/departments")
async def get_departments():
    return select_table(cur, 'departments')


# POST request to load data into the database
@app_globant.post("/departments", status_code=201)
async def add_deparments(file: File, table: Table):

    # Load parameters
    filename = file.filename
    sta_ind = file.sta_ind
    end_ind = file.end_ind
    nrows = end_ind-sta_ind+1
    tablename = table.tablename
    cols_dep = ["id", "name"]

    if (end_ind >= sta_ind) and (nrows <= 1000):  
        # Read file using range of indexes
        df = pd.read_csv(filename, sep=",", names=cols_dep, skiprows=sta_ind-1, nrows=nrows)
        # Process file for inserting in the table
        tuples = [tuple(x) for x in df.to_numpy()]
        cols = ','.join(list(df.columns))
        # Insert data into table
        query = "INSERT INTO %s(%s) VALUES %%s" % (tablename, cols)
        extras.execute_values(cur, query, tuples)
        cxn.commit()
        return select_table(cur, 'departments')

    else:
        if nrows > 1000:
            return {"Consider that 1000 is the maximum amount of registers to load in a single request. You are trying to insert {} registers.".format(nrows)}
        else:
            return {"There is an issue with the indexes. Take into account that end_ind should be greater or equal than sta_ind."}

# # Closing database connection.
# if(cxn):
#     cur.close()
#     cxn.close()
# print("PostgreSQL connection is closed")
