import enum
import os 
import sys 
import uuid
import random
from datetime import datetime

import jsonpickle
import pandas as pd

from sqlalchemy import Column, String, Boolean, DateTime, create_engine, orm, text
from sqlalchemy.orm import declarative_base

import distogram

Base = declarative_base()


@enum.unique
class AggregationType(enum.Enum):
    seconds = 0
    minutes = 1
    hours = 2
    days = 3
    months = 4
    years = 5

    # test for membership: 'every' in AggregationType.__members__


class LabelledDistogram(Base):
    __tablename__ = 'distograms'
    
    primary_key = Column(String, primary_key=True)
    data_source = Column(String)
    variable_name = Column(String)
    datetime = Column(DateTime(timezone=True))
    datetime_min = Column(DateTime(timezone=True))
    distogram_string = Column(String)
    aggregation_type = Column(String)
    temporary_record = Column(Boolean, default=False)

    distogram = distogram.Distogram
    mean = float
    stddev = float

    @property
    def metadata_list(self):
        return [
            self.primary_key,
            self.data_source,
            self.variable_name,
            self.datetime,
            self.datetime_min,
            self.aggregation_type,
            self.temporary_record,
            self.mean,
            self.stddev,
            self.distogram.max,
            self.distogram.min
        ]

    @property
    def metadata_labels(self):
        return [
            "primary_key",
            "data_source",
            "variable_name",
            "datetime",
            "datetime_min",
            "aggregation_type",
            "temporary_record",
            "mean",
            "stdev",
            "max",
            "min"
        ]        
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.distogram_string = jsonpickle.encode(self.distogram)
        self.primary_key = str(uuid.uuid4())
        self.mean = distogram.mean(self.distogram)
        self.stddev = distogram.stddev(self.distogram)

    def __repr__(self):
        return (
            f"<LabelledDistogram(data_source='{self.data_source}', "
            f"primary_key='{self.primary_key}', "
            f"variable_name='{self.variable_name}', "
            f"datetime='{self.datetime}'{type(self.datetime)}, "
            f"datetime_min='{self.datetime_min}'{type(self.datetime_min)}, "
            f"aggregation_type='{self.aggregation_type}', "
            f"temporary_record='{self.temporary_record}', "
            f"min/max='{self.distogram.min}/{self.distogram.max}, "
            f"mean/std='{self.mean}/{self.stddev}'"
        )

    @orm.reconstructor
    def init_on_load(self):
        # define/redefine fields if necessary
        self.distogram = jsonpickle.decode(self.distogram_string)
        self.mean = distogram.mean(self.distogram)
        self.stddev = distogram.stddev(self.distogram)


def data_function(mean=0, sigma=1):
    return random.gauss(mean, sigma)


def make_distribution(n=10000):
    result = []
    for i in range(n):
        result.append(data_function())
    return result


def make_distogram(data):
    h = distogram.Distogram()
    for i in data:
        h = distogram.update(h, i)
    # result = Labelled_Distogram()
    return h
    

def delete_tables(engine, names):
    # Get all tables in the database
    for table in engine.table_names():
        # Delete only the tables in the delete list
        if table in names:
            sql = text("DROP TABLE IF EXISTS {}".format(table))
            engine.execute(sql)


def return_test_engine(database):

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
    elif database == "sqlite-disk-2":
        engine = create_engine('sqlite:///./localdb-2', echo=True)
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


def taxi_datetime(string): 
    return datetime.strptime(string, '%Y-%m-%d %H:%M:%S')


def histogram_step_plot_data(np_hist, columns):
    counts, bins2 = np_hist
    if len(bins2) != len(counts) + 1:
        raise ValueError("histogram data is invalid. len(bins) != len(counts) + 1")
    data = [[bins2[0], 0]]
    data.extend([[bin1, count] for count, bin1 in zip(counts,bins2)])
    data.append([bins2[-1],0])
    df = pd.DataFrame(data, columns=columns)
    return df


taxi_data_headers_map = {
    'tpep_dropoff_datetime': 
        lambda trip: taxi_datetime(trip['tpep_dropoff_datetime']),
    'dropoff_latitude': 
        lambda trip: float(trip['dropoff_latitude']),
    'dropoff_longitude': 
        lambda trip: float(trip['dropoff_longitude']),
    'pickup_latitude': 
        lambda trip: float(trip['pickup_latitude']),
    'pickup_longitude': 
        lambda trip: float(trip['pickup_longitude']),
    'tip_amount': 
        lambda trip: float(trip['tip_amount']),
    'fare_no_tip': 
        lambda trip: float(trip['fare_amount']) - float(trip['tip_amount']),
    'trip_duration': 
        lambda trip: ((
            taxi_datetime(trip['tpep_dropoff_datetime'])
            - taxi_datetime(trip['tpep_pickup_datetime'])).total_seconds())
}

taxi_fields = [
    "total_amount",
    "passenger_count",
    "RateCodeID",
    "dropoff_longitude",
    "VendorID",
    "pickup_latitude",
    "mta_tax",
    "tpep_dropoff_datetime",
    "store_and_fwd_flag",
    "pickup_longitude",
    "tip_amount",
    "dropoff_latitude",
    "improvement_surcharge",
    "trip_distance",
    "payment_type",
    "tpep_pickup_datetime",
    "fare_amount",
    "tolls_amount",
    "polyline",
    "extra",
    "fare_no_tip",
    "trip_duration"
]