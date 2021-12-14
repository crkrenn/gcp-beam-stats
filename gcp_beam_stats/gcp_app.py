#######################
# notes
#######################

# write min_max_queue
# use uuid again?
# object dict

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
import json

from dotenv import load_dotenv

# community
import distogram
import numpy as np
import pandas as pd

from sqlalchemy.orm import sessionmaker

# plotly/dash
import dash
import plotly.express as px
from dash import dcc
from dash import html
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
import plotly.graph_objs as go
from dash import dash_table as dt

# local
from common import (LabelledDistogram, return_test_engine)

# # conditional imports
# try:
#     import zoneinfo
# except ImportError:
#     from backports import zoneinfo

#######################
# Debug
#######################


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


#########################
# Dashboard Layout / View
#########################

def gcp_control(label, choices):
    return dbc.Card(
        [html.Div(
            [dbc.Label(label.replace("_", " ")),
             dcc.Dropdown(
                id=label,
                options=[
                    {"label": item, "value": item} for item in choices],
                value=choices[0],
            )])
         ]
    )


app = dash.Dash(external_stylesheets=[dbc.themes.BOOTSTRAP])

app.layout = dbc.Container(
    [
        html.H1('Streaming summary statistics'),
        html.Hr(),
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
                        df_metadata['aggregation_type'].unique()
                    ), md=3),
            ],
            align='center',
        ),        
        dbc.Row(
            [
                dbc.Col(dcc.Graph(id='time-series-graph'), md=5),
                dbc.Col([dbc.Col(id='min-table')], md=1),
                dbc.Col(dcc.Graph(id='histogram-graph'), md=5),
                dbc.Col([dbc.Col(id='max-table')], md=1),
            ],
            align='center',
        ),
        # dbc.Row(
        #     [
        #         dbc.Col([dbc.Col(id='detail-table')], md=12),
        #     ],
        #     align='center',
        # ),
        dbc.Row(
            [
                dbc.Col(html.Pre(id='click-data'), md=4),
                dbc.Col(html.Pre(id='click-data-2'), md=4),
            ],
            align="center",
        ),
    ],
    fluid=True,
)


#############################################
# Interaction Between Components / Controller
#############################################

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
            mode='markers',  # 'lines' or 'markers'
            name=variable))
    fig.update_layout(clickmode='event+select')
    return fig


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
    if hist == None:
        hist = instance.distogram.bins
    df_hist = pd.DataFrame(np.array(hist), columns=['value', 'count'])
    fig = px.bar(df_hist, x='value', y='count')
    fig.update_layout(clickmode='event+select')
    return fig


@app.callback(
    Output('min-table', 'children'),
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
    return [dt.DataTable(
        data=[{'min': "{:.2e}".format(instance.distogram.min)}],
        columns=[{'name': 'min', 'id': 'min'}],
        style_cell={'fontSize': 12}
    ),
    ]


@app.callback(
    Output('max-table', 'children'),
    [
        Input('data_source', 'value'),
        Input('variable_name', 'value'),
        Input('aggregation_type', 'value'),
        Input('time-series-graph', 'clickData')
    ],
)
def make_max_table(data_source, variable_name, aggregation_type, clickData):
    instance = get_instance(
        data_source, variable_name, aggregation_type, clickData)
    return [dt.DataTable(
        data=[{'max': "{:.2e}".format(instance.distogram.max)}],
        columns=[{'name': 'max', 'id': 'max'}],
        style_cell={'fontSize': 12}
    ),
    ]

# @app.callback(
#     Output('detail-table', 'children'),
#     [
#         Input('data_source', 'value'),
#         Input('variable_name', 'value'),
#         Input('aggregation_type', 'value'),
#         Input('time-series-graph', 'clickData'),
#         Input('histogram-graph', 'clickData')
#     ],
# )
def make_max_table(data_source, variable_name, aggregation_type, clickData):
    instance = get_instance(
        data_source, variable_name, aggregation_type, clickData)
    return [dt.DataTable(
        data=[{'max': "{:.2e}".format(instance.distogram.max)}],
        columns=[{'name': 'max', 'id': 'max'}],
        style_cell={'fontSize': 12}
    ),
    ]

@app.callback(
    Output('click-data', 'children'),
    Input('time-series-graph', 'clickData'))
def display_click_data(clickData):
    return json.dumps(clickData, indent=2)


@app.callback(
    Output('click-data-2', 'children'),
    Input('histogram-graph', 'clickData'))
def display_click_data(clickData):
    return json.dumps(clickData, indent=2)

# start Flask server
# if __name__ == '__main__':
#     app.run_server(
#         debug=True,
#         host='0.0.0.0',
#         port=8050
#     )

if __name__ == '__main__':
    app.run_server(debug=True, port=8890)