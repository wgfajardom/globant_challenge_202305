# app_globant.py

### Import libraries
from fastapi import FastAPI
from pydantic import BaseModel, Field
import pandas as pd

### Initialize API
app_globant = FastAPI()

### HTTP methods for departments

# Function for next department_id
def _find_next_id_dep():
    return max(getattr(elem,'department_id') for elem in departments) + 1

# Class Department
class Department(BaseModel):
    department_id: int = Field(default_factory=_find_next_id_dep, alias="id")
    name: str

# Read departments file and it into a Pandas DataFrame
dep_filename = 'departments.csv'
df_dep = pd.read_csv(dep_filename, names=['id','name'], sep=',')

# Load departments using the class Deparment
departments=[]
for index, row in df_dep.iterrows():
    aux_dep = Department(id=row['id'], name=row['name'])
    departments.append(aux_dep)

# GET request for retrieving the list of departments
@app_globant.get("/departments")
async def get_departments():
    return departments

# @app.post("/countries", status_code=201)
# async def add_country(country: Country):
#     countries.append(country)
#     return country