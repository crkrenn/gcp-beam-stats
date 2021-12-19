#!/usr/bin/env python3
"""
stream data to a Pub/Sub topic
"""


# merge seconds -> minutes
# update seconds -> minutes
# partial aggregation

# logarithmic publishing (every 12 seconds; 5 hours, 6 days, etc.)
# original publish at finest scale
# select, bin, publish, delete
# add old data if appropriate
# when? every agregation_label[1]
# verify that we are not agregating twice. 
# 10 trips in each bin

# print("adding pruned_test data")
# aggregation_lengths = [120, 120, 48, 60, 24, 10]
# aggregation_labels = (
#     ["seconds", "minutes", "hours", "days", "months", "years"])
# date0 = datetime(2021, 10, 31)

# min/max object lists not updating correctly

# dev packages:
import sys

# prod packages
import os
import distogram
from datetime import datetime
import jsonpickle
import time
import pandas as pd

from dateutil.relativedelta import relativedelta

from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from common import (
    Base, return_test_engine, LabelledDistogram, make_distribution, 
    make_distogram, AggregationType, delete_tables, taxi_data_headers_map)

debug = False

try:
    import zoneinfo
except ImportError:
    from backports import zoneinfo

project_id = os.environ.get('DEVSHELL_PROJECT_ID')
dataset = 'default_dataset'
sleep = 1.0  # seconds
batch_size = 10000

# store histograms, time, table, project, 


def prune_test_data(engine):
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    aggregation_lengths = [120, 120, 48, 60, 24, 10]
    aggregation_labels = (
        ["seconds", "minutes", "hours", "days", "months", "years"])



    for instance in (
        session.query(LabelledDistogram).order_by(
            LabelledDistogram.primary_key)):
        pass
    print(
        instance.primary_key,
        instance.variable_name,
        instance.aggregation_type,
        instance.data_source,
        instance.datetime)

def publish_pruned_test_data(engine):
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    if True:
        print("adding pruned_test data")
        aggregation_lengths = [120, 120, 48, 60, 24, 10]
        aggregation_labels = (
            ["seconds", "minutes", "hours", "days", "months", "years"])
        date0 = datetime(2021, 10, 31)
        for i in range(len(AggregationType)):
        # i = 0
        # if True:
            print(AggregationType(i).name)
            j_min = 0
            j_max = 5 * aggregation_lengths[i]
            j_delta = aggregation_lengths[i] // 5
            last_date = date0
            for j in range(j_min, j_max, j_delta):
                delta_dict = {aggregation_labels[i]: -j}
                delta = relativedelta(**delta_dict)
                date = date0 + delta
                # America/New_York
                date.replace(tzinfo=zoneinfo.ZoneInfo('Etc/UTC'))
                h = make_distogram(make_distribution())
                d = LabelledDistogram(
                    data_source="debug3",
                    variable_name="x",
                    datetime_min=last_date,
                    datetime=date,
                    aggregation_type=AggregationType(0).name,
                    temporary_record=False,
                    distogram=h)
                session.add(d)
                session.commit()
                last_date = date
        print("before commit")
        session.commit()
        print("after commit")


def publish_taxi_data(engine):

    def publish_single_time():
        for header, my_distogram in distogram_dict.items():
            d = LabelledDistogram(
                data_source="taxi_min_max",
                variable_name=header,
                datetime=max_datetime,
                aggregation_type=aggregation_type,
                distogram=my_distogram)
            session.add(d)
            session.commit()
        print(f"published data: {max_datetime}")
        print(f"data_source: {d.data_source}")   
             
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    print("taxi!")
    # minutes/hours/days/weeks
    df = pd.read_parquet('data.parquet')
    df.sort_values(by=['tpep_dropoff_datetime'])
    # df2 = df.tail(1000000)['dropoff_latitude']
    # df2.to_csv('latitude.csv')
    # sys.exit()
    aggregation_dict = {
        # "minutes": 15, 
        # "hours": 1, 
        "days": 1}
    for aggregation_type, aggregation_length in aggregation_dict.items():

        delta_time_dict = {aggregation_type: aggregation_length}
        delta_time = relativedelta(**delta_time_dict)
        dt0 = df.iloc[0]['tpep_dropoff_datetime']
        print(f"dt0: {dt0}")
        if aggregation_type in {"minutes", "hours"}:
            dt0 = datetime(*dt0.timetuple()[:4])  # year/month/day/hour
        else:
            dt0 = datetime(*dt0.timetuple()[:3])  # year/month/day
        print(f"dt0: {dt0}")
        min_datetime = dt0
        max_datetime = dt0 + delta_time
        create_new_distograms = True
        for index, row in df.tail(1000000).iterrows():
            if create_new_distograms:
                distogram_dict = {
                    header: distogram.Distogram(with_min_max_list=True) 
                    for header in taxi_data_headers_map
                    if header != 'tpep_dropoff_datetime'}
                create_new_distograms = False
            for header, my_distogram in distogram_dict.items():
                if debug:
                    print(f"json pickle")
                    # print(f"empty: {jsonpickle.encode(my_distogram, indent=2)}")
                    my_distogram = distogram.update(my_distogram, row[header], obj=row.to_dict())
                    # print(f"None max_min: {jsonpickle.encode(my_distogram, indent=2)}")
                    my_distogram = distogram.update(my_distogram, row[header], obj=row.to_dict())
                    print(f"Row max_min: {jsonpickle.encode(my_distogram, indent=2)}")
                    # print(f"Row.to_dict: {jsonpickle.encode(row.to_dict(), indent=2)}")
                    sys.exit()
                # my_distogram = distogram.update(my_distogram, row[header], obj=row.to_dict())
                my_distogram = distogram.update(my_distogram, row[header], obj=row.to_dict())
            if row['tpep_dropoff_datetime'] > max_datetime:
                create_new_distograms = True 
                publish_single_time()
                min_datetime = max_datetime
                max_datetime = max_datetime + delta_time
        publish_single_time()


def main(engine):

    print("before create")
    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    print("before read")
    success = False
    for instance in (
        session.query(LabelledDistogram).order_by(
            LabelledDistogram.primary_key)):
        success = True
        # print(instance.primary_key, instance.variable_name, instance.datetime)


    if success:
        print()
        print(instance.primary_key, instance.variable_name)
        h2 = jsonpickle.decode(instance.distogram_string)
        print(f"min/mean/max {h2.min}/{distogram.mean(h2)}/{h2.max}")

    publish_pruned_test_data(engine)
    prune_test_data(engine)

    # publish_taxi_data(engine)
    # every 
    print("read from database")
    for instance in (
        session.query(LabelledDistogram).order_by(
            LabelledDistogram.primary_key)):
        pass
    print(
        instance.primary_key,
        instance.variable_name,
        instance.aggregation_type,
        instance.data_source,
        instance.datetime)

# from datetime import date
# from dateutil.relativedelta import relativedelta

# six_months = date.today() + relativedelta(months=+6)


if __name__ == "__main__":
    database_list = [
        "bigquery", "sqlite-memory", "sqlite-disk", "sqlite-disk-2", "postgres"]
    database = database_list[3]
    engine = return_test_engine(database)
    print(engine)

    # test(engine)
    main(engine)

