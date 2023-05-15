# Globant Challenge 2023/05
### Public repository dedicated to a Coding Challenge requested by Globant

#### Purpose
The objective of this repository is to solve the Coding Challenge proposed by Globant, which is saved as Guide_Coding_Challenge.pdf. In summary, the challenge has two main tasks divided like this:
  1. Create an API to load data into tables of a database using batch processing.
  2. Create endpoint to solve two business questions using the aformentioned data.
  3. Bonus part.
I was able to solve both main tasks. The bonus part will be ready in the near-future but not for the technical interview (2023/05/14). Anyway, we can discuss it how to do it. I have a couple of ideas about it.

#### Approach
To solve this challenge it was used Python as main programming language. The enitre API is built on top of Python, under the framework FastAPI. The database chosen was PostgreSQL. The connection to the database was established thank to python module psycopg2. Everything was developed on the local machine. 

#### First task
The API allows six different HTTP calls, five of them are GET and the other is POST. The explanation of each one is below:
- (GET) localhost/departments -> retrieves the table departments
- (GET) localhost/jobs -> retrieves the table jobs
- (GET) localhost/hired_employees -> retrieves the table hired_employees
- (GET) localhost/restore_table/{table} -> drop the table {table}, then it is created again
- (GET) localhost/check_schema/{table} -> shows the schema of {table}, useful to know how to use the POST method
- (POST) localhost/add_table/{table} -> load data into the table {table}
For further specifications ask for a demo during the interview.

#### Second task
The queries to solve the business questions were developed on SQL. The API take the query and submit it to PostgreSQL. PostgreSQL returns an answer that is formatted and send to an endpoint. Each business question has its own endpoint.
- (GET) localhost/first_requirement -> answer of the first business question
- (GET) localhost/second_requirement -> answer of the second business question
For further specifications ask for a demo during the interview.

#### Structure of the repository
This repository is composed by several files. Here are their descriptions:

| File | Type | Description
| ----------- | ----------- |  ----------- |
| app_globant.py | Core | Main file of the repository. It contains the source code of the API. |
| local_database_information.txt | Core | Relevant information about how to interact with the database |
| departments.csv | Input | Data to ingest into the departments table |
| jobs.csv | Input | Data to ingest into the jobs table |
| hired_employees.csv | Input | Data to ingest into the hired_employees table |
| app.py | Misc | Template used as reference for the API |
| read_local_data.py | Misc | File useful for manual tests in Python |
| insert_data_local_db.py | Misc | First attemp to connect the API and the database |
| LICENSE | Core | License of the repository |
| README.md | Core | The file you are reading right now |



