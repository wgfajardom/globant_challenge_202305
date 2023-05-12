import pandas as pd
import psycopg2
import subprocess
import sys

CONNECT_DB = "host=localhost port=5432 dbname=globant user=postgres password=GlobantChallenge"

try:
    # Make connection to db
    cxn = psycopg2.connect(CONNECT_DB)
    
    # Create a cursor to db
    cur = cxn.cursor()
    # cur.execute("SELECT * FROM pg_database;")

    # read file, copy to db
    with open('companies.csv', 'r') as f:
        cur.copy_from(f, 'companies', sep=",")
        cxn.commit()

except (Exception, psycopg2.Error) as error:
    print("Error while connecting to PostgreSQL", error)

finally:
    #closing database connection.
    if(cxn):
        cur.close()
        cxn.close()
    print("PostgreSQL connection is closed")
    print("Companies table populated")