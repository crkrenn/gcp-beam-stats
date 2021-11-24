#######################
# notes
#######################

# figure out why first histogram is not the last

# improve formatting time series plot; clean up datetime labels

# consider logarithmic time axis: 
# https://community.plotly.com/t/plotting-time-dates-on-a-log-axis/33205/7

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
import json

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
import plotly.graph_objs as go
from dash import dash_table as dt

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
# Debug
#######################

print([[name, member, member.value] for name, member in AggregationType.__members__.items()])

#######################
# Data Analysis / Model
#######################

# choose database (currently postgres)
load_dotenv()
database_list = [
    "bigquery", "sqlite-memory", "sqlite-disk", "postgres"]
database = database_list[2]
engine = return_test_engine(database)

# load initial data
Session = sessionmaker(bind=engine)
session = Session()
labelled_distogram_list = list(
    session.query(LabelledDistogram).order_by(
        LabelledDistogram.datetime)) 
labelled_distogram_hash = {
    instance.primary_key: instance for instance in labelled_distogram_list}

df_metadata = pd.DataFrame(
    [instance.metadata_list for instance in labelled_distogram_list],
    columns=labelled_distogram_list[0].metadata_labels)


df2 = pd.read_csv('https://git.io/Juf1t')
df2 = df2[["State"]]
# print(df2.to_dict('records'))


# app.layout = dbc.Container([
#     dbc.Label('Click a cell in the table:'),
#     dt.DataTable(
#         id='tbl', data=df.to_dict('records'),
#         columns=[{"name": i, "id": i} for i in df.columns],
#     ),
#     dbc.Alert(id='tbl_out'),
# ])

#########################
# Dashboard Layout / View
#########################


app = dash.Dash(external_stylesheets=[dbc.themes.BOOTSTRAP])


def gcp_control(label, choices):
    return dbc.Card(
        [
            html.Div(
                [
                    dbc.Label(label.replace("_", " ")),
                    dcc.Dropdown(
                        id=label,
                        options=[
                            {"label": item, "value": item} 
                                for item in choices
                        ],
                        value=choices[0],
                    ),
                ]
            )
        ]
    )


app.layout = dbc.Container(
    [
        html.H1('Streaming statistics'),
        html.Hr(),
        # gcp_controls,
        dbc.Row(
            [
                dbc.Col(
                    gcp_control(
                        'data_source', 
                        df_metadata['data_source'].unique()
                    ), md=3),
                dbc.Col(
                    gcp_control(
                        'variable_name', 
                        df_metadata['variable_name'].unique()
                    ), md=3),

                dbc.Col(
                    gcp_control(
                        'aggregation_type', 
                        [name for name, member 
                            in AggregationType.__members__.items()]
                    ), md=3),
            ],
            align='center',
        ),        
        dbc.Row(
            [
                dbc.Col(dcc.Graph(id='time-series-graph'), md=4),
                dbc.Col(dt.DataTable(
                    id='tbl2', 
                    data=[{'min': -2}],
                    columns=[{'name': 'min', 'id': 'min'}],
                    # data=df2.to_dict('records'),
                    # columns=[{'name': i, 'id': i} for i in df2.columns],
                    style_cell={'fontSize': 12}
                    ), md=1),
                dbc.Col([
                    # dt.DataTable(
                    #     data=[{'min': 0}],
                    #     columns=[{'name': 'min', 'id': 'min'}],
                    #     # columns=[{'name': i, 'id': i} for i in df.columns],
                    # ),
                    # dt.DataTable(
                    # id='tbl3', 
                    # data=[{'min': -2}],
                    # columns=[{'name': 'min', 'id': 'min'}],
                    # # data=df2.to_dict('records'),
                    # # columns=[{'name': i, 'id': i} for i in df2.columns],
                    # style_cell={'fontSize': 12}
                    # ), 
                    dbc.Col(id='tbl4')
                    ],md=1),
                # dbc.Col(dt.DataTable(id='min-table'), md=1),
                        # dt.DataTable(min-table

        #     id='tbl', data=df2.to_dict('records'),
        #     columns=[{'name': i, 'id': i} for i in df2.columns],
        # ),
    #                 dt.DataTable(
    #     id='tbl', data=df.to_dict('records'),
    #     columns=[{'name': i, 'id': i} for i in df.columns],
    # ),
                dbc.Col(dcc.Graph(id='histogram-graph'), md=6),
                # dbc.Col(dcc.Graph(id='cluster-graph'), md=4),
                # dbc.Col(dcc.Graph(id='histogram-graph'), md=4),
            ],
            align='center',
        ),

        dt.DataTable(
            id='tbl', data=df2.to_dict('records'),
            columns=[{'name': i, 'id': i} for i in df2.columns],
        ),
        # html.Div([
        #     dcc.Markdown('''
        #         **Click Data**

        #         Click on points in the graph.
        #     """),
        #     html.Pre(id='click-data', style=styles['pre']),
        # ], className='three columns'),
        dbc.Row(
            [
                dbc.Col(html.Pre(id='click-data'), md=4),
            ],
            align="center",
        ),
    ],
    fluid=True,
)


#############################################
# Interaction Between Components / Controller
#############################################

@app.callback(
    Output('time-series-graph', 'figure'),
    [
        Input('data_source', 'value'),
        Input('variable_name', 'value'),
        Input('aggregation_type', 'value'),
    ],
)
def make_time_series_graph(data_source, variable_name, aggregation_type):
    df = df_metadata
    df_subset = df.loc[
        (df['data_source'] == data_source) 
        & (df['variable_name'] == variable_name)
        & (df['aggregation_type'] == aggregation_type)]
    # Create figure
    fig = go.Figure()
    x_label = 'datetime'
    variable_list = ['max', 'mean', 'min']
    # Loop df columns and plot columns to the figure
    for variable in variable_list:
        fig.add_trace(go.Scatter(
            x=df_subset[x_label], 
            y=df_subset[variable],
            customdata=df_subset['primary_key'],
            mode='markers', # 'lines' or 'markers'
            name=variable))
    fig.update_layout(clickmode='event+select')
    return fig


def get_instance(data_source, variable_name, aggregation_type, clickData):
    ctx = dash.callback_context
    trigger = ctx.triggered[0]['prop_id'].split('.')[0]

    if clickData == None or trigger != 'time-series-graph':
        df = df_metadata
        df_subset = df.loc[
            (df['data_source'] == data_source) 
            & (df['variable_name'] == variable_name)
            & (df['aggregation_type'] == aggregation_type)]
        instance = labelled_distogram_hash[df_subset.iloc[-1].primary_key]
    else:
        instance = labelled_distogram_hash[
            clickData['points'][0]['customdata']]
    return instance

@app.callback(
    Output('histogram-graph', 'figure'),
    [
        Input('data_source', 'value'),
        Input('variable_name', 'value'),
        Input('aggregation_type', 'value'),
        Input('time-series-graph', 'clickData')
    ],
)
def make_histogram(data_source, variable_name, aggregation_type, clickData):
    instance = get_instance(
        data_source, variable_name, aggregation_type, clickData)
    hist = distogram.histogram(instance.distogram)
    df_hist = pd.DataFrame(np.array(hist), columns=['bin', 'count'])
    return px.bar(df_hist, x='bin', y='count', title='distogram')
    # fig = plotly_histogram(df_hist)
    return json.dumps(clickData, indent=2)


@app.callback(
    Output('click-data', 'children'),
    Input('time-series-graph', 'clickData'))
def display_click_data(clickData):
    return json.dumps(clickData, indent=2)


@app.callback(
    Output('tbl4', 'children'),
    [
        Input('data_source', 'value'),
        Input('variable_name', 'value'),
        Input('aggregation_type', 'value'),
        Input('time-series-graph', 'clickData')
    ],
)
def make_min_table(data_source, variable_name, aggregation_type, clickData):
    instance = get_instance(
        data_source, variable_name, aggregation_type, clickData)
    return [ dt.DataTable(
        data=[{'min': instance.mean}],
        columns=[{'name': 'min', 'id': 'min'}],
        # columns=[{'name': i, 'id': i} for i in df.columns],
    ),
    ]

                # dbc.Col([
                #     dt.DataTable(
                #     id='tbl3', 
                #     data=[{'min': -2}],
                #     columns=[{'name': 'min', 'id': 'min'}],
                #     # data=df2.to_dict('records'),
                #     # columns=[{'name': i, 'id': i} for i in df2.columns],
                #     style_cell={'fontSize': 12}
                #     ), 
                #     dbc.Col(id='tbl4')
                #     ],md=1),

# app.layout = dash_table.DataTable(
#    id = 'datatable-paging',
#    columns = [{
#          "name": i,
#          "id": i
#       }
#       for i in sorted(df.columns)
#    ],
#    page_current = 0,
#    page_size = PAGE_SIZE,
#    page_action = 'custom'
# )

# @app.callback(
#    Output('datatable-paging', 'data'),
#    Input('datatable-paging', "page_current"),
#    Input('datatable-paging', "page_size"))
# def update_table(page_current, page_size):
#    return df.iloc[
#       page_current * page_size: (page_current + 1) * page_size
#    ].to_dict('records')

# html.Div([
#     dt.DataTable(
#             rows=df.to_dict('records'),
#             columns=df.columns,
#             row_selectable=True,
#             filterable=True,
#             sortable=True,
#             selected_row_indices=list(df.index),  # all rows selected by default
#             id='2'
#      ),
#     html.Button('Submit', id='button'),
#     html.Div(id="div-1"),
# ])


# @app.callback(
#     dash.dependencies.Output('div-1', 'children'),
#     [dash.dependencies.Input('button', 'n_clicks')])
# def update_output(n_clicks):

#     df_chart = df.groupby('Name').sum()

#     return [
#         dt.DataTable(
#             rows=df_chart.to_dict('rows'),
#             columns=df_chart.columns,
#             row_selectable=True,
#             filterable=True,
#             sortable=True,
#             selected_row_indices=list(df_chart.index),  # all rows selected by default
#             id='3'
#         )
#     ]
# start Flask server
# if __name__ == '__main__':
#     app.run_server(
#         debug=True,
#         host='0.0.0.0',
#         port=8050
#     )

if __name__ == '__main__':
    app.run_server(debug=True, port=8890)