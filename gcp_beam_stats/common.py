import enum
import os 
import sys 
import uuid
import random

import jsonpickle
import distogram

from sqlalchemy import Column, String, DateTime, Enum, create_engine, orm
from sqlalchemy.orm import declarative_base

Base = declarative_base()


@enum.unique
class AggregationType(enum.Enum):
    every = 0
    minutely = 1
    hourly = 2
    daily = 3
    monthly = 4
    yearly = 5

    # test for membership: 'every' in AggregationType.__members__


class LabelledDistogram(Base):
    __tablename__ = 'distograms'
    
    primary_key = Column(String, primary_key=True)
    data_source = Column(String)
    variable_name = Column(String)
    datetime = Column(DateTime(timezone=True))
    distogram_string = Column(String)
    aggregation_type = Enum(AggregationType)

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
            self.aggregation_type,
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
            "aggregation_type",
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
            f"aggregation_type='{self.aggregation_type}', "
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