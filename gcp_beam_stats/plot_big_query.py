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

# export DEVSHELL_PROJECT_ID=interview-study-<tab>
# export GOOGLE_APPLICATION_CREDENTIALS=~/.google_cloud/interview-study-<tab>


project_id = os.environ.get('DEVSHELL_PROJECT_ID')
dataset = 'default_dataset'

engine = create_engine(f'bigquery://{project_id}/{dataset}')

with engine.connect() as conn:
    conn.execute(text("DROP TABLE IF EXISTS some_table"))
    conn.execute(text("CREATE TABLE some_table (x int, y int)"))
    conn.execute(
        text("INSERT INTO some_table (x, y) VALUES (:x, :y)"),
        [{"x": 1, "y": 1}, 
         {"x": 2, "y": 3},
         {"x": 3, "y": 7},
         {"x": 4, "y": 2},
         {"x": 5, "y": 1}
         ]
    )


with engine.connect() as conn:
    result = conn.execute(text("SELECT x, y FROM some_table"))
    for row in result:
        print(f"x: {row.x}  y: {row.y}")

