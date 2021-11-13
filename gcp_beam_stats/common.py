import enum
import uuid
import random

import jsonpickle
import distogram

from sqlalchemy import Column, String, DateTime, Enum
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

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.distogram_string = jsonpickle.encode(self.distogram)
        self.primary_key = str(uuid.uuid4())

    def __repr__(self):
        return (
            f"<LabelledDistogram(data_source='{self.data_source}', "
            f"primary_key='{self.primary_key}', "
            f"variable_name='{self.variable_name}', "
            f"datetime='{self.datetime}', "
            f"aggregation_type='{self.aggregation_type}', "
            f"min/max='{self.distogram.min}/{self.distogram.max}")


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
    