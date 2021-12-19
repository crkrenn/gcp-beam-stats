#######################
# notes
#######################

# fix min/max date
# replace min/max tables with min/max/mean/(count)more (count)?
# make detail table
# fix hang in database creation
# work on thinning database

# Future development/polishing:
# track representative points for each histogram bin as well as min/max

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

# dtructure

#######################
# python imports
#######################

# system
import json
import math

from dotenv import load_dotenv

# community
import distogram
# import numpy as np
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
from common import (
    LabelledDistogram, return_test_engine, histogram_step_plot_data)


def is_interactive():
    import __main__ as main
    return not hasattr(main, '__file__')


if is_interactive():
    print("interactive!")
    from jupyter_dash import JupyterDash
   
database_list = [
    "bigquery", "sqlite-memory", "sqlite-disk", "sqlite-disk-2", "postgres"]
database = database_list[3]
engine = return_test_engine(database)

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
# moved to begining of file
# database_list = [
#     "bigquery", "sqlite-memory", "sqlite-disk", "postgres"]
# database = database_list[2]
# engine = return_test_engine(database)

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
                searchable=False,
            )])
         ]
    )

if is_interactive():
    app = JupyterDash(external_stylesheets=[dbc.themes.BOOTSTRAP])
else:
    app = dash.Dash(external_stylesheets=[dbc.themes.BOOTSTRAP])

app.layout = dbc.Container(
    [
        html.H1('{Streaming} summary statistics'),
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
                dbc.Col(dcc.Graph(id='time-series-graph'), md=4),
                dbc.Col([dbc.Col(id='min-table')], md=2),
                dbc.Col(dcc.Graph(id='histogram-graph'), md=5),
                # dbc.Col([dbc.Col(id='max-table')], md=2),
            ],
            align='center',
        ),
        dbc.Row(
            [
                dbc.Col([dbc.Col(id='max-table')], md=12),
            ],
            align='center',
        ),
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
    fig.update_xaxes(title_text="date/time")
    fig.update_yaxes(title_text=variable_name)
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
    hist = distogram.frequency_density_distribution(instance.distogram)
    datetime_min = instance.datetime_min
    datetime = instance.datetime
    if hist == None:
        fig = px.scatter(
            x=[instance.distogram.bins[0][0]],
            y=[1],
            labels={"x": variable_name, "y": "count"}
            )
        return fig
    labels = ["bin", "count/bin width"]
    df_hist = histogram_step_plot_data(hist, labels)
    fig = px.line(
        df_hist,
        x=labels[0],
        y=labels[1],
        labels = {labels[0]: variable_name},
        log_y=True,
        title=f"From: {datetime_min}<br>To: {datetime}"
    )
    fig.update_traces(mode="lines", line_shape="hv")
    fig.update_layout(clickmode='event+select')
    return fig

def make_min_max_table(
    data_source, variable_name, aggregation_type, clickData,
    min_max_type):
    if min_max_type not in ["min", "max"]:
        raise ValueError(
            f"{min_max_type} is not supported. "
            f"Only 'min' and 'max' are supported.")
    instance = get_instance(
        data_source, variable_name, aggregation_type, clickData)
    if min_max_type == "min":
        min_max_data = distogram.min_list(instance.distogram)
    elif min_max_type == "max":
        min_max_data = distogram.max_list(instance.distogram)
    # calculate necessary significant figures
    if len(min_max_data) > 1:
        min_data = min(min_max_data)
        max_data = max(min_max_data)
        range_list = [min_max_data[i] - min_max_data[i-1] for i in range(1,len(min_max_data))]
        range_list.sort()
        range_list = [i for i in range_list if i > 0]
        max_abs_data = max([abs(min_data), abs(max_data)])
        sig_figs_needed = -int(math.log10(range_list[0]/max_abs_data))
        format_string = "{" + f":.{sig_figs_needed}e" + "}"
    else:
        format_string = "{:.2e}"

    df = pd.DataFrame(
        [format_string.format(i) 
                for i in min_max_data],
        columns=[min_max_type])
    return [dt.DataTable(
        columns=[{"name": i, "id": i} for i in df.columns],
        data=df.to_dict('records'),
        style_cell={'fontSize': 12},
    ),
    ]

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
    return make_min_max_table(
        data_source, variable_name, aggregation_type, clickData, min_max_type="min")

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
    return make_min_max_table(
        data_source, variable_name, aggregation_type, clickData, min_max_type="max")


@app.callback(
    Output('click-data', 'children'),
    Input('histogram-graph', 'clickData'))
def display_click_data(clickData):
    return json.dumps(clickData, indent=2)


@app.callback(
    Output('click-data-2', 'children'),
    Input('histogram-graph', 'clickData'))
    # Input('min-table', "derived_virtual_selected_rows"))

    # Input('min-table', 'selected_columns'))
def display_click_data(selected_columns):
    return json.dumps(selected_columns, indent=2)

# # table with click callback
# from dash import Dash, Input, Output, callback
# from dash import dash_table as dt
# # import pandas as pd
# # import dash_bootstrap_components as dbc

# df = pd.read_csv('https://git.io/Juf1t')
# # df = df["State"]
# df=df[["State"]]
# app = JupyterDash(external_stylesheets=[dbc.themes.BOOTSTRAP])

# app.layout = dbc.Container([
#     dbc.Label('Click a cell in the table:'),
#     dt.DataTable(
#         id='tbl', data=df.to_dict('records'),
#         columns=[{"name": i, "id": i} for i in df.columns],
#     ),
#     dbc.Alert(id='tbl_out'),
# ])

# @callback(Output('tbl_out', 'children'), Input('tbl', 'active_cell'))
# def update_graphs(active_cell):
#     return str(active_cell) if active_cell else "Click the table"

# # if __name__ == "__main__":
# #     app.run_server(debug=True)
# run(app)


#############################################
# Interaction Between Components / Controller
#############################################


# start Flask server
# if __name__ == '__main__':
#     app.run_server(
#         debug=True,
#         host='0.0.0.0',
#         port=8050
#     )

if __name__ == '__main__':
    if is_interactive():
        app.run_server(mode='inline')
    else:
        app.run_server(debug=True, port=8893)
    
