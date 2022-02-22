#!/usr/bin/env python3
"""
stream data to a Pub/Sub topic
"""
# @TODO: add tests
# @TODO: add class for datetime floor


# with session.begin():
#     session.add(some_object())
#     session.add(some_other_object())
# # commits transaction at the end, or rolls back if there
# # was an exception raised

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

# def ic(*a):
#     print(*a)
#     return (
#         None if not a else (a[0] if len(a) == 1 else a)  # noqa
#     )

def is_interactive():
    import __main__ as main
    return not hasattr(main, '__file__')



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
# datetime_aggregation_labels = (
#     ["seconds", "minutes", "hours", "days", "months", "years"])
# date0 = datetime(2021, 10, 31)

# min/max object lists not updating correctly

# dev packages:
import sys

# prod packages
import os
import distogram
from datetime import datetime
from datetime import timedelta
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

datetime_aggregation_labels_delta = (
    "seconds", "minutes", "hours", "days", "months", "years")
datetime_aggregation_labels_round = (
    "second", "minute", "hour", "day", "month", "year")
datetime_delta_to_round_dict = {
    delta_label: round_label for delta_label, round_label in zip(
        datetime_aggregation_labels_delta, datetime_aggregation_labels_round)}


def datetime_floor(datetime, aggregation_type='seconds'):
    """ Rounds datetime down to previous `aggregation_type` """
    aggregation_index = (
        datetime_aggregation_labels_delta.index(aggregation_type))
    rounding_dict = {"microsecond": 0}
    for j in range(aggregation_index):
        rounding_dict[datetime_aggregation_labels_round[j]] = 0
    return datetime.replace(**rounding_dict) 


def make_aggregation_subsets_by_time(
    df_input,
    min_column='datetime_min',
    max_column='datetime',
    aggregation_type='minutes',
    aggregation_length=1,
    min_time=None,
    max_time=None,
):
    """ 
    Divides dataframe into subsets based on time.

    Minimum aggregation type is minutes.

    Args:
        df: [Required] A dataframe.
        min_column: [Optional] Name of column with min time value.
        max_column: [Optional] Name of column with max time value.
        aggregation_type: [Optional] Type of aggregation. Must be 
                          valid input to dateutil.relativedelta.
        agregation_length: Size of aggregation.
        min_time: Minimum time to return in agregations.
        max_time: Maximum time to return in agregations. 

    Returns:
        A list of dataframes.
    """
    # @TODO: generalize for arbitrary aggregation
    # @TODO: Consider outputing an interable instead of a list
    df = df_input.copy()
    df['parsed'] = False
    df.sort_values(by=[min_column], inplace=True)
    df['_delta_time'] = df[max_column] - df[min_column]

    delta_time_dict = {aggregation_type: aggregation_length}
    first_time = df.iloc[0].at[min_column]
    last_time = df.iloc[-1].at[max_column]

    aggregation_index = (
        datetime_aggregation_labels_delta.index(aggregation_type))
    assert aggregation_index > 0

    merge_start_time = last_time - relativedelta(seconds=1)
    merge_start_time = datetime_floor(
        merge_start_time, aggregation_type=aggregation_type)
    result = []
    while merge_start_time >= first_time - relativedelta(**delta_time_dict):
        merge_stop_time = merge_start_time + relativedelta(**delta_time_dict)
        df_mask = ((df[min_column] >= merge_start_time)
                   & (df[min_column] <= merge_stop_time)
                   & (df['_delta_time'] <= timedelta(**delta_time_dict)))
        df_merge = df[df_mask]
        df.loc[df_mask, 'parsed'] = True
        if not df_merge.empty:
            result.append(df_merge)
            merge_start_time = (
                merge_start_time - relativedelta(**delta_time_dict))
        else:
            merge_start_time = df[df['parsed'] == False].iloc[-1].at[max_column]
            merge_start_time = merge_start_time - relativedelta(seconds=1)
            merge_start_time = datetime_floor(
                merge_start_time, aggregation_type=aggregation_type)
    return result


def prune_test_data(engine):
    # aggregation: min <= time < max 
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    datetime_aggregation_lengths = [120, 120, 48, 60, 24, 10]
    datetime_aggregation_labels = (
        ["seconds", "minutes", "hours", "days", "months", "years"])

    # load initial data
    labelled_distogram_list = list(
        session.query(LabelledDistogram).order_by(
            LabelledDistogram.datetime)) 
    labelled_distogram_hash = {
        instance.primary_key: instance for instance in labelled_distogram_list}

    df_metadata = pd.DataFrame(
        [instance.metadata_list for instance in labelled_distogram_list],
        columns=labelled_distogram_list[0].metadata_labels)

    # first_time = df_metadata.iloc[0].at['datetime_min']
    # last_time = df_metadata.iloc[-1].at['datetime'] 
    # delta_time_dict = {
    #     datetime_aggregation_labels[aggregation_index]: aggregation_lengths[aggregation_index]}
    # min_keep_time = last_time - relativedelta(**delta_time_dict)

    aggregation_type = 'minutes'
    prune_type = datetime_aggregation_labels.index(aggregation_type)
    last_time = 

    aggregation_subsets = make_aggregation_subsets_by_time(
        df_metadata,
        min_column='datetime_min',
        max_column='datetime',
        aggregation_type=aggregation_type,
        aggregation_length=1,
    )


    # df_metadata['__delta_time'] = (
    #     df_metadata['datetime'] - df_metadata['datetime_min'])
    # ic(df_metadata['__delta_time'])
    # ic(type(timedelta(seconds=2)))
    # ic(df_metadata[df_metadata['__delta_time'] < timedelta(seconds=1)]['__delta_time'])
    # df_metadata['parsed'] = False
    
    # combine seconds to minutes
    # @TODO: writing time rounding function
    # aggregation_index = 0
    # df_fine = df_metadata[
    #     df_metadata['aggregation_type'] == datetime_aggregation_labels[aggregation_index]]
    # # df_fine.sort_values(by=['datetime_min'], inplace=True)
    # # df_fine.reset_index(drop=True, inplace=True)


    # delta_time_dict = {datetime_aggregation_labels[aggregation_index + 1]: 1}

    # merge_stop_time = last_time
    # merge_start_time = merge_stop_time - relativedelta(**delta_time_dict)
    # while merge_start_time > first_time:
    # # if True:
    # #
    # # for i in range(30):
    #     # merge_start_time = merge_stop_time - relativedelta(**delta_time_dict)
    #     rounding_dict = {"microsecond": 0}
    #     for j in range(aggregation_index + 1):
    #         rounding_dict[datetime_aggregation_labels[j][:-1]] = 0
    #     merge_start_time = merge_stop_time - relativedelta(seconds=1)
    #     merge_start_time = merge_start_time.replace(**rounding_dict)
    #     merge_stop_time = merge_start_time + relativedelta(**delta_time_dict)
        
    #     df_mask = ((df_fine["datetime_min"] >= merge_start_time)
    #                & (df_fine["datetime"] <= merge_stop_time))
    #     df_merge = df_fine[df_mask]
    #     df_fine.loc[df_mask, "parsed"] = True
    #     ic(merge_start_time, merge_stop_time)
    #     ic(df_merge[["datetime_min", "datetime"]])
    #     if not df_merge.empty:
    #         merge_stop_time = merge_start_time
    #     else:
    #         merge_stop_time = df_fine[df_fine["parsed"] == False].iloc[-1].at['datetime']
    # print("done")


def publish_pruned_test_data(engine):
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    if True:
        print("adding pruned_test data")
        aggregation_lengths = [120, 120, 48, 60, 24, 10]
        datetime_aggregation_labels = (
            ["seconds", "minutes", "hours", "days", "months", "years"])
        date0 = datetime(2021, 10, 31)
        for i in range(len(AggregationType)):
        # i = 0
        # if True:
            print(AggregationType(i).name)
            j_min = 1
            j_max = 5 * aggregation_lengths[i]
            j_delta = aggregation_lengths[i] // 5
            for j in range(j_min, j_max, j_delta):
                delta_time_dict = {datetime_aggregation_labels[i]: -j}
                delta = relativedelta(**delta_time_dict)
                date = date0 + delta
                delta_time_dict = {datetime_aggregation_labels[0]: 1}
                delta = relativedelta(**delta_time_dict)
                date_min = date - delta
                # America/New_York
                date.replace(tzinfo=zoneinfo.ZoneInfo('Etc/UTC'))
                h = make_distogram(make_distribution())
                d = LabelledDistogram(
                    data_source="debug3",
                    variable_name="x",
                    datetime_min=date_min,
                    datetime=date,
                    aggregation_type=AggregationType(0).name,
                    temporary_record=False,
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
    df.sort_values(by=['tpep_dropoff_datetime'], inplace=True)
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
    
        # publish_pruned_test_data(engine)
#     prune_test_data(engine)

# from datetime import date
# from dateutil.relativedelta import relativedelta

# six_months = date.today() + relativedelta(months=+6)


if __name__ == "__main__":
    import os
    database_list = [
        "bigquery", "sqlite-memory", "sqlite-disk", "sqlite-disk-2", "postgres"]
    database = database_list[3]
    engine = return_test_engine(database)
    print(engine)

    # test(engine)
#     main(engine)
#     os.remove("./localdb-2")
#     publish_pruned_test_data(engine)
    #
    prune_test_data(engine)
    print("done")
