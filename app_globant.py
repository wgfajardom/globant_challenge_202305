# app_globant.py

### Import libraries
from fastapi import FastAPI
from pydantic import BaseModel, Field
from typing import Optional, Union
import pandas as pd
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

# Class Table
class Table(BaseModel):
    tablename: str
    tableschema: dict

# Class Department
class Department(BaseModel):
    id: int
    department: str

# Class Department
class Job(BaseModel):
    id: int
    job: str

# Class Department
class Hired_Employee(BaseModel):
    id: int
    name: Union[str, None]
    datetime: Union[str, None]
    department_id: Union[int, None]
    job_id: Union[int, None]

# Class Requeriment_1
class Requeriment_1(BaseModel):
    department: str
    job: str
    q1: int
    q2: int
    q3: int
    q4: int

# Class Requeriment_2
class Requeriment_2(BaseModel):
    id: int
    department: str
    hired: int



### Core functions for HTTP methods

# Retriving data from database (used by a GET method)
def retrieve_table(cur, table):
    cur.execute("SELECT * FROM {} ORDER BY id;".format(table))
    cxn.commit()
    raw_ls = list(cur.fetchall())
    if len(raw_ls) > 0:
        if table=="departments":
            output_ls = [Department(id=elem[0], department=elem[1]) for elem in raw_ls]
        if table=="jobs":
            output_ls = [Job(id=elem[0], job=elem[1]) for elem in raw_ls]
        if table=="hired_employees":
            output_ls = [Hired_Employee(id=elem[0], name=elem[1], datetime=elem[2], department_id=elem[3], job_id=elem[4]) for elem in raw_ls]
        return output_ls
    else:
        return {"The table {} is empty".format(table)}




# Delete and create again a table (used by a GET method)
def restore_tb(cur, table):
    if table in ["departments", "jobs", "hired_employees"]:
        # Delete table
        cur.execute("DROP TABLE {};".format(table))
        # Create empty table
        if table=="departments":
            cur.execute("CREATE TABLE {}( ID INT PRIMARY KEY NOT NULL, DEPARTMENT TEXT );".format(table))
        if table=="jobs":
            cur.execute("CREATE TABLE {}( ID INT PRIMARY KEY NOT NULL, JOB TEXT );".format(table))
        if table=="hired_employees":
            cur.execute("CREATE TABLE {}( ID INT PRIMARY KEY NOT NULL, NAME TEXT, DATETIME TEXT, DEPARTMENT_ID INT, JOB_ID INT );".format(table))
        cxn.commit()
        # Result
        return {"Restored table {}. Now it's empty.".format(table)}
    else:
        return {"This table does not belong to the database."}



# Check schema of a table (used by a GET method)
def check_sch(cur, table):
    if table in ["departments", "jobs", "hired_employees"]:
        # Retrieve table schema
        cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{}' ORDER By ordinal_position;".format(table))
        cxn.commit()
        retrieved_schema = list(cur.fetchall())
        # Result
        url_post = "http://127.0.0.1:8000/{}".format(table)
        return {
            "table_name":"{}".format(table), 
            "table_schema":"{}".format(retrieved_schema)
            }
    else:
        return {"This table does not belong to the database."}



# Batch load to database (used by a POST method)
def batch_load(cur, filename, sta_ind, end_ind, tablename, tableschema):

    # Derived variables
    nrows = end_ind-sta_ind+1
    cols_table = list(tableschema.keys())

    # Apply conditions regarding the size batch and the order of the indexes
    if (sta_ind >= 1) and (end_ind >= sta_ind) and (nrows <= 1000):  

        # Read file using range of indexes
        df = pd.read_csv(filename, sep=",", names=cols_table, skiprows=sta_ind-1, nrows=nrows)
        
        # Replacing null values by placeholders (null values cannot be inserted into the table so far)
        null_ph_int = -999
        null_ph_str = 'NULL'
        dc_null_placeholders = dict()
        for key, value in tableschema.items():
            if value == 'int':
                dc_null_placeholders[key] = null_ph_int
            else:
                dc_null_placeholders[key] = null_ph_str
        df = df.fillna(value=dc_null_placeholders)

        # Change schema based on the schema of the destination table
        try:
            df = df.astype(tableschema)
        except:
            url_get_schema = ""
            return {"Schema not valid. Verify the schema of the destination table. For this use http://127.0.0.1:8000/check_schema/{}".format(tablename)}
            sys.exit()

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
        for key, value in tableschema.items():
            if value == 'int':
                cur.execute("UPDATE {} SET {} = NULL WHERE {} = {};".format(tablename, key, key, dc_null_placeholders[key]))
            else:
                cur.execute("UPDATE {} SET {} = NULL WHERE {} = '{}';".format(tablename, key, key, dc_null_placeholders[key]))
        cxn.commit()

        # Response
        url_get_table = "http://127.0.0.1:8000/{}".format(tablename)
        la_ids = len(allow_ids)
        ln_ids = len(nonal_ids)
        if (la_ids > 0) and (ln_ids > 0):
            response = {"Batch load successful. In total {} registers were inserted. They correspond to ids {}. There were {} registers that already exist on table {}, corresponding to ids {}. Check your results using {}".format(la_ids, allow_ids, ln_ids, tablename, nonal_ids, url_get_table)}
        elif (ln_ids == 0):
            response = {"Batch load successful. In total {} registers were inserted. They correspond to ids {}. Any existing register overlaps with new ones, or there were not existing registers on table {}. Check your results using {}.".format(la_ids, allow_ids, tablename, url_get_table)}
        elif (la_ids == 0):
            response = {"Batch load unsuccessful. No registers were inserted, because all of them already exist on table {}. Check the table using {}".format(tablename, url_get_table)}
        else:
            response = {"Somehow you broke this API. At least some text is shown."}

        return response
    
    else:
        # Exceptions due to indexes issues or batch size
        if nrows > 1000:
            return {"Consider that 1000 is the maximum amount of registers to load in a single request. You are trying to insert {} registers.".format(nrows)}
        elif sta_ind <= 0:
            return {"The parameter sta_ind must be greater or equal than 1."}
        else:
            return {"There is an issue with the indexes. Take into account that end_ind should be greater or equal than sta_ind."}



def requeriment_1(cur):

    # Read input data
    cur.execute("SELECT * FROM departments;")
    cxn.commit()
    ls_dep = list(cur.fetchall())
    cur.execute("SELECT * FROM jobs;")
    cxn.commit()
    ls_job = list(cur.fetchall())
    cur.execute("SELECT * FROM hired_employees;")
    cxn.commit()
    ls_hie = list(cur.fetchall())

    # Verify existence of input data
    if (len(ls_dep) == 0) or (len(ls_job) == 0) or (len(ls_hie) == 0):
    
        return {"It is not possible to execute the requirement. One or more of these tables (departments, jobs, hired_employees) do not have data loaded. Please load the respective data using http://127.0.0.1:8000/add_table"}
    
    else:

        # Query definition
        query = """
        SELECT  
            department, 
            job,
            sum((aux_table.quarter = 'Q1')::int) AS Q1,
            sum((aux_table.quarter = 'Q2')::int) AS Q2,
            sum((aux_table.quarter = 'Q3')::int) AS Q3,
            sum((aux_table.quarter = 'Q4')::int) AS Q4
        FROM (
            SELECT  
                d.department AS department,
                j.job AS job,
                CONCAT('Q',CAST(EXTRACT(QUARTER FROM CAST(h.datetime AS DATE)) AS TEXT)) AS quarter
            FROM hired_employees AS h
            INNER JOIN departments AS d 
            ON h.department_id = d.id
            INNER JOIN jobs AS j
            ON h.job_id = j.id
            WHERE (h.datetime IS NOT NULL) AND (EXTRACT(YEAR FROM CAST(h.datetime AS DATE)) = 2021)
            ) AS aux_table
        GROUP BY department, job
        ORDER BY department, job
        ;"""

        # Query execution
        cur.execute(query)
        cxn.commit()
        result_query_ls = list(cur.fetchall())
        output = [Requeriment_1(department=elem[0], job=elem[1], q1=elem[2], q2=elem[3], q3=elem[4], q4=elem[5]) for elem in result_query_ls]

        return output



def requeriment_2(cur):

    # Read input data
    cur.execute("SELECT * FROM departments;")
    cxn.commit()
    ls_dep = list(cur.fetchall())
    cur.execute("SELECT * FROM hired_employees;")
    cxn.commit()
    ls_hie = list(cur.fetchall())

    # Verify existence of input data
    if (len(ls_dep) == 0)  or (len(ls_hie) == 0):
    
        return {"It is not possible to execute the requirement. One or both of these tables (departments and hired_employees) do not have data loaded. Please load the respective data using http://127.0.0.1:8000/add_table"}
    
    else:

        # Query definition
        query = """
        SELECT 
            h.department_id AS id,
            d.department AS department,
            COUNT(h.id) AS hired
        FROM hired_employees AS h
        INNER JOIN departments AS d
        ON h.department_id=d.id
        WHERE (EXTRACT(YEAR FROM CAST(h.datetime AS DATE)) = 2021) AND (h.department_id IS NOT NULL)
        GROUP BY h.department_id, department
        HAVING COUNT(h.id) > (SELECT ( CAST( (SELECT COUNT(id) AS number_employees FROM hired_employees WHERE (EXTRACT(YEAR FROM CAST(datetime AS DATE)) = 2021) AND (department_id IS NOT NULL)) AS FLOAT) / CAST( (SELECT COUNT(id) AS number_departments FROM departments) AS FLOAT ) ))
        ORDER BY hired DESC
        ;"""

        # Query execution
        cur.execute(query)
        cxn.commit()
        result_query_ls = list(cur.fetchall())
        output = [Requeriment_2(id=elem[0], department=elem[1], hired=elem[2]) for elem in result_query_ls]

        return output




### HTTP methods

# GET method for retrieving the list of departments
@app_globant.get("/departments")
async def get_departments():
    # Call retrieve_table function
    return retrieve_table(cur, 'departments')


# GET method for retrieving the list of jobs
@app_globant.get("/jobs")
async def get_jobs():
    # Call retrieve_table function
    return retrieve_table(cur, 'jobs')


# GET method for retrieving the list of departments
@app_globant.get("/hired_employees")
async def get_hired_employees():
    # Call retrieve_table function
    return retrieve_table(cur, 'hired_employees')


# GET method for restoring a table
@app_globant.get("/restore_table/{table}")
async def restore_table(table):
    # Call restore_tb function
    return restore_tb(cur, table)


# GET method for restoring a table
@app_globant.get("/check_schema/{table}")
async def check_schema(table):
    # Call check_sch function
    return check_sch(cur, table)


# POST method to load data of a table into the database
@app_globant.post("/add_table", status_code=201)
async def add_deparments(file: File, table: Table):
    # Load parameters
    filename = file.filename
    sta_ind = file.sta_ind
    end_ind = file.end_ind
    tablename = table.tablename
    tableschema = table.tableschema
    # Call batch_load function
    return batch_load(cur, filename, sta_ind, end_ind, tablename, tableschema)


# GET method for requeriment 1
@app_globant.get("/first_requirement")
async def first_requirement():
    # Call requeriment_1 function
    return requeriment_1(cur)

# GET method for requeriment 2
@app_globant.get("/second_requirement")
async def second_requirement():
    # Call requeriment_2 function
    return requeriment_2(cur)
