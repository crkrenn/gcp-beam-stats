#!/usr/bin/env python3

import os
import time

from sqlalchemy import *
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *

from sqlalchemy import text

# schema:
#    timestamp
#    agregation size: seconds, minutes, hours, days, months, years, infinite
# 

project_id = os.environ.get('DEVSHELL_PROJECT_ID')
dataset = 'default_dataset'

engine = create_engine(f'bigquery://{project_id}/{dataset}')

# with engine.connect() as conn:
#     result = conn.execute(text("select 'hello world'"))
#     print(result.all())

with engine.connect() as conn:
    # conn.execute(text("DROP TABLE IF EXISTS some_table"))
    # conn.execute(text("CREATE TABLE some_table (x int, y int)"))
    # conn.execute(
    #     text("INSERT INTO some_table (x, y) VALUES (:x, :y)"),
    #     [{"x": 1, "y": 1}, 
    #      {"x": 2, "y": 3},
    #      {"x": 3, "y": 7},
    #      {"x": 4, "y": 2},
    #      {"x": 5, "y": 1}
    #      ]
    # )
    time.sleep(5)    
    conn.execute(
        text("INSERT INTO some_table (x, y) VALUES (:x, :y)"),
        [{"x": 6, "y": 2}])
    time.sleep(5)
    conn.execute(
        text("INSERT INTO some_table (x, y) VALUES (:x, :y)"),
        [{"x": 7, "y": 3}])
    time.sleep(5)
    conn.execute(
        text("INSERT INTO some_table (x, y) VALUES (:x, :y)"),
        [{"x": 8, "y": 4}])
    time.sleep(5)
    conn.execute(
        text("INSERT INTO some_table (x, y) VALUES (:x, :y)"),
        [{"x": 9, "y": 5}])
# table = Table(
#     'mytable', ..., 
#     bigquery_description='my table description', 
#     bigquery_friendly_name='my table friendly name')

# table = Table('dataset.table', MetaData(bind=engine), autoload=True)
# print(select([func.count('*')], from_obj=table).scalar())
