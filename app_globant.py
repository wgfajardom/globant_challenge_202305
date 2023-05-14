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

# Class Department
class Department(BaseModel):
    department_id: int = Field(alias="id")
    name: str




### Core functions for HTTP methods

# Retriving data from database (for GET methods)
def retrieve_table(cur, table):
    cur.execute("SELECT * FROM {} ORDER BY id;".format(table))
    raw_ls = list(cur.fetchall())
    if len(raw_ls) > 0:
        output_ls = [Department(id=elem[0], name=elem[1]) for elem in raw_ls]
    else:
        output_ls = []
    return output_ls


# Batch load to database (for POST methods)
def batch_load(cur, filename, sta_ind, end_ind, cols_table):

    nrows = end_ind-sta_ind+1
    tablename = filename.split(".")[0]

    if (sta_ind >= 1) and (end_ind >= sta_ind) and (nrows <= 1000):  

        # Read file using range of indexes
        df = pd.read_csv(filename, sep=",", names=cols_table, skiprows=sta_ind-1, nrows=nrows)

        # List of ids to insert
        input_ids = set(list(df["id"].values))

        # List of existing ids in the destination table
        cur.execute("SELECT id FROM {};".format(tablename))
        exist_ids = set([tup[0] for tup in cur.fetchall()])

        # Filter ids that already exist on the table
        if exist_ids != {}:
            allow_ids = list(input_ids.difference(exist_ids))
            nonal_ids = list(input_ids.intersection(exist_ids))
            df = df[df["id"].isin(allow_ids)]
        else:
            allow_ids = list(input_ids)
            nonal_ids = []

        # Process file for inserting in the table
        tuples = [tuple(x) for x in df.to_numpy()]
        cols = ','.join(list(df.columns))

        # Insert data into table
        query = "INSERT INTO %s(%s) VALUES %%s" % (tablename, cols)
        extras.execute_values(cur, query, tuples)
        cxn.commit()

        # Response
        url_get_departments = "http://127.0.0.1:8000/{}".format(tablename)
        la_ids = len(allow_ids)
        ln_ids = len(nonal_ids)
        if (la_ids > 0) and (ln_ids > 0):
            response = {"Batch load successful. In total {} registers were inserted. They correspond to ids {}. There were {} registers that already exist on table {}, corresponding to ids {}. Check your results using {}".format(la_ids, allow_ids, ln_ids, tablename, nonal_ids, url_get_departments)}
        elif (ln_ids == 0):
            response = {"Batch load successful. In total {} registers were inserted. They correspond to ids {}. There were not existing registers on table {}. Check your results using {}.".format(la_ids, allow_ids, tablename, url_get_departments)}
        elif (la_ids == 0):
            response = {"Batch load unsuccessful. No registers were inserted, because all of them already exist on table {}. Check the table using {}".format(tablename, url_get_departments)}
        else:
            response = {"Somehow you broke this API. At least some text is shown."}

        return response

    else:
        if nrows > 1000:
            return {"Consider that 1000 is the maximum amount of registers to load in a single request. You are trying to insert {} registers.".format(nrows)}
        elif sta_ind <= 0:
            return {"The parameter sta_ind must be greater or equal than 1."}
        else:
            return {"There is an issue with the indexes. Take into account that end_ind should be greater or equal than sta_ind."}



### HTTP methods

# GET request for retrieving the list of departments
@app_globant.get("/departments")
async def get_departments():
    # Call retrieve_table function
    return retrieve_table(cur, 'departments')


# POST request to load data into the database
@app_globant.post("/departments", status_code=201)
async def add_deparments(file: File):
    
    # Load parameters
    filename = file.filename
    sta_ind = file.sta_ind
    end_ind = file.end_ind

    # Parameter exclusive for the table departments
    cols_table = ["id", "name"]

    # Call batch_load function
    return batch_load(cur, filename, sta_ind, end_ind, cols_table)



# # Closing database connection.
# if(cxn):
#     cur.close()
#     cxn.close()
# print("PostgreSQL connection is closed")
