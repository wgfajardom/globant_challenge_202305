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
    department_id: int = Field(alias="dep_id")
    name: str

# Class Department
class Job(BaseModel):
    job_id: int = Field(alias="job_id")
    name: str

# Class Department
class Hired_Employee(BaseModel):
    hired_employee_id: int = Field(alias="hie_id")
    name: str
    datetime: str
    department_id: int
    job_id: int



### Core functions for HTTP methods

# Retriving data from database (used by a GET method)
def retrieve_table(cur, table):
    cur.execute("SELECT * FROM {} ORDER BY id;".format(table))
    cxn.commit()
    raw_ls = list(cur.fetchall())
    print(raw_ls)
    if len(raw_ls) > 0:
        if table=="departments":
            output_ls = [Department(dep_id=elem[0], name=elem[1]) for elem in raw_ls]
        if table=="jobs":
            output_ls = [Job(job_id=elem[0], name=elem[1]) for elem in raw_ls]
        if table=="hired_employees":
            output_ls = [Hired_Employee(hie_id=elem[0], name=elem[1], datetime=elem[2], department_id=elem[3], job_id=elem[4]) for elem in raw_ls]
    else:
        output_ls = {"The table {} do not exist in the database.".format(table)}
    return output_ls



# Delete and create again a table (used by a GET method)
def restore_table(cur, table):
    if table in ["departments", "jobs", "hired_employees"]:
        # Delete table
        cur.execute("DROP TABLE {};".format(table))
        # Create empty table
        if table=="departments":
            cur.execute("CREATE TABLE {}( ID INT PRIMARY KEY NOT NULL, NAME TEXT );".format(table))
        if table=="jobs":
            cur.execute("CREATE TABLE {}( ID INT PRIMARY KEY NOT NULL, NAME TEXT );".format(table))
        if table=="hired_employees":
            cur.execute("CREATE TABLE {}( ID INT PRIMARY KEY NOT NULL, NAME TEXT, DATETIME TEXT, DEPARTMENT_ID INT, JOB_ID INT );".format(table))
        cxn.commit()
        # Result
        return {"Restored table {}. Now it's empty.".format(table)}
    else:
        return {"This table does not belong to the database."}



# Batch load to database (used by a POST method)
def batch_load(cur, filename, sta_ind, end_ind, cols_table):

    nrows = end_ind-sta_ind+1
    tablename = filename.split(".")[0]

    if (sta_ind >= 1) and (end_ind >= sta_ind) and (nrows <= 1000):  

        # Read file using range of indexes
        df = pd.read_csv(filename, sep=",", names=cols_table, skiprows=sta_ind-1, nrows=nrows)
        df = df.fillna(int(-999))

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
        cur.execute("UPDATE hired_employees SET department_id = NULL WHERE department_id = -999;")
        cur.execute("UPDATE hired_employees SET job_id = NULL WHERE job_id = -999;")
        cxn.commit()

        # Response
        url_get_table = "http://127.0.0.1:8000/{}".format(tablename)
        la_ids = len(allow_ids)
        ln_ids = len(nonal_ids)
        if (la_ids > 0) and (ln_ids > 0):
            response = {"Batch load successful. In total {} registers were inserted. They correspond to ids {}. There were {} registers that already exist on table {}, corresponding to ids {}. Check your results using {}".format(la_ids, allow_ids, ln_ids, tablename, nonal_ids, url_get_table)}
        elif (ln_ids == 0):
            response = {"Batch load successful. In total {} registers were inserted. They correspond to ids {}. Any existing result overlap with new ones, or there were not existing registers on table {}. Check your results using {}.".format(la_ids, allow_ids, tablename, url_get_table)}
        elif (la_ids == 0):
            response = {"Batch load unsuccessful. No registers were inserted, because all of them already exist on table {}. Check the table using {}".format(tablename, url_get_table)}
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


# GET request for retrieving the list of jobs
@app_globant.get("/jobs")
async def get_jobs():
    # Call retrieve_table function
    return retrieve_table(cur, 'jobs')


# GET request for retrieving the list of departments
@app_globant.get("/hired_employees")
async def get_hired_employees():
    # Call retrieve_table function
    return retrieve_table(cur, 'hired_employees')


# GET request for restoring a table
@app_globant.get("/restore/{table}")
async def restore(table):
    # Call restore_table function
    return restore_table(cur, table)


# POST request to load data of the table departments into the database
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


# POST request to load data of the table jobs into the database
@app_globant.post("/jobs", status_code=201)
async def add_jobs(file: File):
    # Load parameters
    filename = file.filename
    sta_ind = file.sta_ind
    end_ind = file.end_ind
    # Parameter exclusive for the table jobs
    cols_table = ["id", "name"]
    # Call batch_load function
    return batch_load(cur, filename, sta_ind, end_ind, cols_table)


# POST request to load data of the table hired_employees into the database
@app_globant.post("/hired_employees", status_code=201)
async def add_hired_employees(file: File):
    # Load parameters
    filename = file.filename
    sta_ind = file.sta_ind
    end_ind = file.end_ind
    # Parameter exclusive for the table hired_employees
    cols_table = ["id", "name", "datetime", "department_id", "job_id"]
    # Call batch_load function
    return batch_load(cur, filename, sta_ind, end_ind, cols_table)
