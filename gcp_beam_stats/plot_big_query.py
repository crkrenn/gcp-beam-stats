#!/usr/bin/env python3

# https://stackoverflow.com/questions/63459424/how-to-add-multiple-graphs-to-dash-app-on-a-single-browser-page
# https://medium.com/analytics-vidhya/building-a-dashboard-app-using-plotlys-dash-a-complete-guide-from-beginner-to-pro-61e890bdc423
# https://dash.plotly.com/live-updates

import os

import distogram
import jsonpickle

import numpy as np
import pandas as pd
import plotly.express as px

import dash
import dash_core_components as dcc
# import dash_html_components as html
from dash import html

from sqlalchemy.orm import sessionmaker

from common import (
    LabelledDistogram, make_distribution, make_distogram, return_test_engine)


def read_distogram(engine):
    Session = sessionmaker(bind=engine)
    session = Session()

    instance = session.query(LabelledDistogram).first() 
    return jsonpickle.decode(instance.distogram_string)


def main(engine):
    h = read_distogram(engine)
    # h = make_distogram(make_distribution())
    print(f"min/mean/max {h.min}/{distogram.mean(h)}/{h.max}")
    # plotly and dash
    hist = distogram.histogram(h)
    df_hist = pd.DataFrame(np.array(hist), columns=["bin", "count"])
    fig = px.bar(df_hist, x="bin", y="count", title="distogram")    
    # # plotly
    # fig.update_layout(height=300)
    # fig.show()
    # dash

    # colors = {
    #     'background': '#111111',
    #     'text': '#7FDBFF'
    # }
    # fig.update_layout(
    #     plot_bgcolor=colors['background'],
    #     paper_bgcolor=colors['background'],
    #     font_color=colors['text']
    # )

    app = dash.Dash(__name__)
    
    app.layout = html.Div(children=[
        html.H1(children='Hello There Dash'),

        html.Div(children='''
            Dash: A web application framework for your data.
        '''),

        dcc.Graph(
            id='example-graph',
            figure=fig
        )
    ])

    # app.layout = html.Div(style={'backgroundColor': colors['background']}, children=[
    #     html.H1(
    #         children='Hello Dash',
    #         style={
    #             'textAlign': 'center',
    #             'color': colors['text']
    #         }
    #     ),

    #     html.Div(children='Dash: A web application framework for your data.', style={
    #         'textAlign': 'center',
    #         'color': colors['text']
    #     }),

    #     dcc.Graph(
    #         id='example-graph-2',
    #         figure=fig
    #     )
    # ])

    # def generate_table(dataframe, max_rows=10):
    #     return html.Table([
    #         html.Thead(
    #             html.Tr([html.Th(col) for col in dataframe.columns])
    #         ),
    #         html.Tbody([
    #             html.Tr([
    #                 html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
    #             ]) for i in range(min(len(dataframe), max_rows))
    #         ])
    #     ])


    # app = dash.Dash(__name__)

    # app.layout = html.Div([
    #     html.H4(children='US Agriculture Exports (2011)'),
    #     generate_table(df)
    # ])

    return app


if __name__ == "__main__":
    database_list = [
        "bigquery", "sqlite-memory", "sqlite-disk", "postgres"]
    database = database_list[3]

    engine = return_test_engine(database)

    app = main(engine)
    app.run_server(debug=True)

