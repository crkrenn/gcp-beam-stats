#!/usr/bin/env python3

import os

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

with engine.connect() as conn:
    result = conn.execute(text("select 'hello world'"))
    print(result.all())

with engine.connect() as conn:
    conn.execute(text("CREATE TABLE some_table (x int, y int)"))
    conn.execute(
        text("INSERT INTO some_table (x, y) VALUES (:x, :y)"),
        [{"x": 1, "y": 1}, {"x": 2, "y": 4}]
    )

# table = Table(
#     'mytable', ..., 
#     bigquery_description='my table description', 
#     bigquery_friendly_name='my table friendly name')

# table = Table('dataset.table', MetaData(bind=engine), autoload=True)
# print(select([func.count('*')], from_obj=table).scalar())
