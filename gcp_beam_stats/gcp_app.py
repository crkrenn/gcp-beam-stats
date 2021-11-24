#######################
# notes
#######################

# improve formatting time series

# time series from postgres; histogram from time series point
# see "shiny_example_2"

# MVP: time series/histogram/extrema/integration/table
# 12: extrema 1; 6/1/4/1

# dash plan:
# row 0 controls
# row 1 min/histogram/max
# row 2 time series/table

# consider row 1 time series/histogram

# controls: integrated/discrete; variable

# functions: select data

# data structure

#######################
# python imports
#######################

# system
import os
import sys
import pickle
import base64
import time

from dotenv import load_dotenv
from datetime import datetime

# community
import distogram
import jsonpickle
import numpy as np
import pandas as pd

from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# plotly/dash
import dash
import plotly.express as px
from dash import dcc
from dash import html
from jupyter_dash import JupyterDash
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc


# shiny_app imports
import plotly.graph_objs as go
from sklearn import datasets
from sklearn.cluster import KMeans

# local
from common import (Base,
    LabelledDistogram, make_distribution, make_distogram, AggregationType,
    return_test_engine
    )

# conditional imports
try:
    import zoneinfo
except ImportError:
    from backports import zoneinfo

#######################
# Data Analysis / Model
#######################

# choose database (currently postgres)
load_dotenv()
database_list = [
    "bigquery", "sqlite-memory", "sqlite-disk", "postgres"]
database = database_list[3]
engine = return_test_engine(database)

# load initial data
Session = sessionmaker(bind=engine)
session = Session()
labelled_distogram_list = (
    session.query(LabelledDistogram).order_by(
        LabelledDistogram.primary_key)) 
labelled_distogram_hash = {
    instance.primary_key: instance for instance in labelled_distogram_list}

df_metadata = pd.DataFrame(
    [instance.metadata_list for instance in labelled_distogram_list],
    columns=labelled_distogram_list[0].metadata_labels)

# shiny_app code
iris_raw = datasets.load_iris()
iris = pd.DataFrame(iris_raw["data"], columns=iris_raw["feature_names"])

#########################
# Dashboard Layout / View
#########################


app = dash.Dash(external_stylesheets=[dbc.themes.BOOTSTRAP])


def gcp_control(label):
    return dbc.Card(
        [
            html.Div(
                [
                    dbc.Label(label.replace("_", " ")),
                    dcc.Dropdown(
                        id=label,
                        options=[
                            {"label": item, "value": item} 
                                for item in df_metadata[label].unique()
                        ],
                        value=df_metadata[label].unique()[0],
                    ),
                ]
            )
        ]
    )


# controls = dbc.Card(
#     [
#         html.Div(
#             [
#                 dbc.Label("X variable"),
#                 dcc.Dropdown(
#                     id="x-variable",
#                     options=[
#                         {"label": col, "value": col} for col in iris.columns
#                     ],
#                     value="sepal length (cm)",
#                 ),
#             ]
#         ),
#         html.Div(
#             [
#                 dbc.Label("Y variable"),
#                 dcc.Dropdown(
#                     id="y-variable",
#                     options=[
#                         {"label": col, "value": col} for col in iris.columns
#                     ],
#                     value="sepal width (cm)",
#                 ),
#             ]
#         ),
#         html.Div(
#             [
#                 dbc.Label("Cluster count"),
#                 dbc.Input(id="cluster-count", type="number", value=3),
#             ]
#         ),
#     ],
#     body=True,
# )

app.layout = dbc.Container(
    [
        html.H1("Iris k-means clustering"),
        html.Hr(),
        # gcp_controls,
        dbc.Row(
            [
                dbc.Col(dcc.Graph(id="time-series-graph"), md=4),
                # dbc.Col(dcc.Graph(id="cluster-graph"), md=4),
                # dbc.Col(dcc.Graph(id="histogram-graph"), md=4),
            ],
            align="center",
        ),
        dbc.Row(
            [
                dbc.Col(gcp_control("data_source"), md=4),
                dbc.Col(gcp_control("variable_name"), md=4),
            ],
            align="center",
        ),
        # dbc.Row(
        #     [
        #         dbc.Col(controls, md=4),
        #     ],
        #     align="center",
        # ),
    ],
    fluid=True,
)


#############################################
# Interaction Between Components / Controller
#############################################

@app.callback(
    Output("time-series-graph", "figure"),
    [
        Input("data_source", "value"),
        Input("variable_name", "value"),
    ],
)
def make_time_series_graph(data_source, variable_name):
    df = df_metadata
    df_subset = df.loc[
        (df["data_source"] == data_source) 
        & (df["variable_name"] == variable_name)]
    fig = px.scatter(
        df_subset, 
        x="datetime",
        # y=["max", "mean", "min"]
        y="mean"
    )
    # Create figure
    fig = go.Figure()
    x_label = "datetime"
    variable_list = ["max", "mean", "min"]
    # Loop df columns and plot columns to the figure
    for variable in variable_list:
        fig.add_trace(go.Scatter(x=df_subset[x_label], y=df_subset[variable],
                            mode='markers', # 'lines' or 'markers'
                            name=variable))
    return fig
    # Add traces
    # fig.add_trace(go.Scatter(x=random_x, y=random_y0,
    #                     mode='markers',
    #                     name='markers'))
    # fig.add_trace(go.Scatter(x=random_x, y=random_y1,
    #                     mode='lines+markers',
    #                     name='lines+markers'))
    # fig.add_trace(go.Scatter(x=random_x, y=random_y2,
    #                     mode='lines',
    #                     name='lines'))
    # return fig

# @app.callback(
#     Output("cluster-graph", "figure"),
#     [
#         Input("x-variable", "value"),
#         Input("y-variable", "value"),
#         Input("cluster-count", "value"),
#     ],
# )
# def make_graph(x, y, n_clusters):
#     # minimal input validation, make sure there's at least one cluster
#     km = KMeans(n_clusters=max(n_clusters, 1))
#     df = iris.loc[:, [x, y]]
#     km.fit(df.values)
#     df["cluster"] = km.labels_

#     centers = km.cluster_centers_

#     data = [
#         go.Scatter(
#             x=df.loc[df.cluster == c, x],
#             y=df.loc[df.cluster == c, y],
#             mode="markers",
#             marker={"size": 8},
#             name="Cluster {}".format(c),
#         )
#         for c in range(n_clusters)
#     ]

#     data.append(
#         go.Scatter(
#             x=centers[:, 0],
#             y=centers[:, 1],
#             mode="markers",
#             marker={"color": "#000", "size": 12, "symbol": "diamond"},
#             name="Cluster centers",
#         )
#     )

#     layout = {"xaxis": {"title": x}, "yaxis": {"title": y}}

#     return go.Figure(data=data, layout=layout)

# @app.callback(
#     Output("histogram-graph", "figure"),
#     [
#         Input("x-variable", "value"),
#         Input("y-variable", "value"),
#         Input("cluster-count", "value"),
#     ],
# )
# def make_histogram(x, y, n_clusters):
#     # minimal input validation, make sure there's at least one cluster
#     km = KMeans(n_clusters=max(n_clusters, 1))
#     df = iris.loc[:, [x, y]]
#     km.fit(df.values)
#     df["cluster"] = km.labels_

#     centers = km.cluster_centers_

#     data = [
#         go.Scatter(
#             x=df.loc[df.cluster == c, x],
#             y=df.loc[df.cluster == c, y],
#             mode="markers",
#             marker={"size": 8},
#             name="Cluster {}".format(c),
#         )
#         for c in range(n_clusters)
#     ]

#     data.append(
#         go.Scatter(
#             x=centers[:, 0],
#             y=centers[:, 1],
#             mode="markers",
#             marker={"color": "#000", "size": 12, "symbol": "diamond"},
#             name="Cluster centers",
#         )
#     )

#     layout = {"xaxis": {"title": x}, "yaxis": {"title": y}}

#     return go.Figure(data=data, layout=layout)


# Filter options

# # make sure that x and y values can't be the same variable
# def filter_options(v):
#     """Disable option v"""
#     return [
#         {"label": col, "value": col, "disabled": col == v}
#         for col in iris.columns
#     ]


# # functionality is the same for both dropdowns, so we reuse filter_options
# app.callback(Output("x-variable", "options"), [Input("y-variable", "value")])(
#     filter_options
# )
# app.callback(Output("y-variable", "options"), [Input("x-variable", "value")])(
#     filter_options
# )


# start Flask server
# if __name__ == '__main__':
#     app.run_server(
#         debug=True,
#         host='0.0.0.0',
#         port=8050
#     )

if __name__ == "__main__":
    app.run_server(debug=True, port=8890)