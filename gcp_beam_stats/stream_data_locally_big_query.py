#!/usr/bin/env python3
"""
stream data to a Pub/Sub topic
"""

import os
import sys
# import time
# from typing import Callable
import distogram
from datetime import datetime
import pickle
import jsonpickle
# from dataclasses import dataclass
import base64
import time

from dateutil.relativedelta import relativedelta

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from common import (Base,
    LabelledDistogram, make_distribution, make_distogram, AggregationType)


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



def return_test_engine(string):

    project_id = os.environ.get('DEVSHELL_PROJECT_ID')
    postgres_user = os.environ.get('POSTGRES_USER')
    postgres_password = os.environ.get('POSTGRES_PASSWORD')
    dataset = 'default_dataset'

    if database == "bigquery":
        engine = create_engine(f'bigquery://{project_id}/{dataset}')
    elif database == "sqlite-memory":
        engine = create_engine('sqlite:///:memory:', echo=True)
    elif database == "sqlite-disk":
        engine = create_engine('sqlite:///./localdb', echo=True)
    elif database == "postgres":
        if not postgres_user:
            print(f"ERROR: postgres_user {postgres_user} is not defined.")
            sys.exit()
        else:
            engine = create_engine(
                f'postgresql://{postgres_user}:{postgres_password}'
                '@localhost:5432/google_cloud_dev')
    else:
        print(f"ERROR: database {database} is not recognized")
        sys.exit()
    return engine


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


def main(engine):

    print("before delete")
    if False:
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS distograms"))
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
        print(instance.primary_key, instance.variable_name, instance.datetime)

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
            datetime=now ,
            aggregation_type="every",
            distogram=h)
        print()
        print(d)
        session.add(d)
        session.commit()
        time.sleep(3)

    # every = 1 120
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
            print(AggregationType(i).name)
            for j in range(aggregation_lengths[i]):
                delta_dict = {aggregation_labels[i]: -j}
                delta = relativedelta(**delta_dict)
                date = date0 + delta
                date.replace(tzinfo=zoneinfo.ZoneInfo('Etc/UTC'))
                h = make_distogram(make_distribution())
                d = LabelledDistogram(
                    data_source="pruned_test",
                    variable_name="x",
                    datetime=date,
                    aggregation_type=AggregationType(i).name,
                    distogram=h)
                session.add(d)
                session.commit()
        print("before commit")
        session.commit()
        print("after commit")
    print("read from database")
    for instance in (
        session.query(LabelledDistogram).order_by(
            LabelledDistogram.primary_key)):
        print(
            instance.primary_key,
            instance.variable_name,
            instance.datetime)


# from datetime import date
# from dateutil.relativedelta import relativedelta

# six_months = date.today() + relativedelta(months=+6)

if __name__ == "__main__":
    database_list = [
        "bigquery", "sqlite-memory", "sqlite-disk", "postgres"]
    database = database_list[3]

    engine = return_test_engine(database)

    test(engine)
    main(engine)
