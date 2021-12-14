#!/usr/bin/env python3
"""
stream data to a Pub/Sub topic
"""

# min/max object lists not updating correctly

# dev packages:
import sys
debug = False

# prod packages
import os
# import time
# from typing import Callable
import distogram
from datetime import datetime
import jsonpickle
# from dataclasses import dataclass
import time
import pandas as pd

from dateutil.relativedelta import relativedelta

from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from common import (
    Base, return_test_engine, LabelledDistogram, make_distribution, 
    make_distogram, AggregationType, delete_tables, taxi_data_headers_map)


try:
    import zoneinfo
except ImportError:
    from backports import zoneinfo


project_id = os.environ.get('DEVSHELL_PROJECT_ID')
dataset = 'default_dataset'
sleep = 1.0  # seconds
batch_size = 10000


# store histograms, time, table, project, 

  # * beam: store distograms on big_query
  #   * test data
  #   * data on pubsub source
  #   * datetime
  #   * variable_name
  #   * function to extract data





def test(engine):

    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)

    session = Session()

    data = make_distribution()
    h = make_distogram(data)
    print(f"min/max {h.min} {h.max}")
    now = datetime.utcnow()
    now = now.replace(tzinfo=zoneinfo.ZoneInfo('Etc/UTC'))
    print(now)

    # h_pickle = pickle.dumps(h)
    # h_base64 = base64.b64encode(h_pickle).decode('ascii')
    # print("h_base64")
    # print(len(h_base64))
    # h2_pickle = base64.b64decode(h_base64.encode('ascii'))
    # h2 = pickle.loads(h2_pickle)
    # print(f"min/mean/max {h2.min}/{distogram.mean(h2)}/{h2.max}")

    # h_json = jsonpickle.encode(h)
    # print("h_json")
    # print(len(h_json))
    # # print(h_json)
    # h2 = jsonpickle.decode(h_json)
    # print(f"min/mean/max {h2.min}/{distogram.mean(h2)}/{h2.max}")

    d = LabelledDistogram(
        data_source="dev",
        variable_name="x",
        datetime=now ,
        aggregation_type="every",
        distogram=h)
    d2 = LabelledDistogram(
        data_source="dev",
        variable_name="x",
        datetime=now ,
        aggregation_type="every",
        distogram=h)

    print(f"d: {d}")
    print(f"d2: {d2}")
    print(f"h_pickle: {len(d.distogram_string)}")
    print(f"d.__table__\n{d.__table__}")

    session.add(d)
    session.add(d2)

    print(f"session.new: {session.new}")
    session.commit()
    print(f"session.new: {session.new}")
    print(f"d: {d}")
    print(f"d2: {d2}")

    for instance in (
        session.query(LabelledDistogram).order_by(
            LabelledDistogram.primary_key)):
        print(instance.primary_key, instance.variable_name)

    # primary_key = Column(Sequence('user_id_seq'), primary_key=True)
    # data_source = Column(String)
    # variable_name = Column(String)
    # datetime = Column(DateTime(timezone=True))
    # distogram_string = Column(String)
    # aggregation_type = Enum(AggregationType)

def publish_pruned_test_data(engine):
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    # secondly = 1 120
    # minutely = 2 120
    # hourly = 3 48
    # daily = 4 60
    # monthly = 5 24
    # yearly = 6 10
    if False:
        print("adding pruned_test data")
        aggregation_lengths = [120, 120, 48, 60, 24, 10]
        aggregation_labels = (
            ["seconds", "minutes", "hours", "days", "months", "years"])
        date0 = datetime(2021, 10, 31)
        for i in range(len(AggregationType)):
        # i = 0
        # if True:
            print(AggregationType(i).name)
            for j in range(aggregation_lengths[i]):
                delta_dict = {aggregation_labels[i]: -j}
                delta = relativedelta(**delta_dict)
                date = date0 + delta
                # America/New_York
                date.replace(tzinfo=zoneinfo.ZoneInfo('Etc/UTC'))
                h = make_distogram(make_distribution())
                d = LabelledDistogram(
                    data_source="debug2",
                    variable_name="x",
                    datetime=date,
                    aggregation_type=AggregationType(i).name,
                    distogram=h)
                session.add(d)
                session.commit()
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
        max_datetime = dt0 + delta_time
        create_new_distograms = True
        for index, row in df.tail(1000000).iterrows():
            if create_new_distograms:
                distogram_dict = {
                    header: distogram.Distogram(min_max_record=True) 
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
                max_datetime = max_datetime + delta_time
        publish_single_time()


def main(engine):

    print("before delete")
    if False:
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS distograms"))
    if False:
        delete_tables(engine, "LabelledDistogram")
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

    while False:

        h = make_distogram(make_distribution())
        now = datetime.utcnow()
        now = now.replace(tzinfo=zoneinfo.ZoneInfo('Etc/UTC'))
        d = LabelledDistogram(
            data_source="dev",
            variable_name="x",
            datetime=now,
            aggregation_type="every",
            distogram=h)
        print()
        print(d)
        session.add(d)
        session.commit()
        time.sleep(3)

    # publish_pruned_test_data(engine)

    publish_taxi_data(engine)
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
        "bigquery", "sqlite-memory", "sqlite-disk", "postgres"]
    database = database_list[2]

    engine = return_test_engine(database)

    # test(engine)
    main(engine)

            # delta_dict = {aggregation_type: aggregation_length}
            #     delta = relativedelta(**delta_dict)
            #     date = date0 + delta
            #     # America/New_York
            #     date.replace(tzinfo=zoneinfo.ZoneInfo('Etc/UTC'))
        # i = 0
        # # if True:
        #     print(AggregationType(i).name)
        #     for j in range(aggregation_lengths[i]):
        #         delta_dict = {aggregation_labels[i]: -j}
        #         delta = relativedelta(**delta_dict)
        #         date = date0 + delta
        #         date.replace(tzinfo=zoneinfo.ZoneInfo('Etc/UTC'))
        #         h = make_distogram(make_distribution())
        #         d = LabelledDistogram(
        #             data_source="debug2",
        #             variable_name="x",
        #             datetime=date,
        #             aggregation_type=AggregationType(i).name,
        #             distogram=h)
        #         session.add(d)
        #         session.commit()
        # print("before commit")
        # session.commit()
        # print("after commit")

