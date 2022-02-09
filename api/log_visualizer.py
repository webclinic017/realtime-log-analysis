# %%
"""
# Log File - Data Visualization
"""

# %%
"""
### Loading Libraries
##### Spark Session, Dataframe Functions, Pandas, Dash, Dash core components, html components, Input, Output and state dependencies, Plotly Graph objects, time and date time
"""

# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pandas as pd
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import plotly.graph_objs as go
from datetime import datetime,timedelta
import time

# %%
"""
### Spark Session
##### Spark Session object creation with configuration data stax spark-cassandra connector and cassandra related connectivity credentials.
"""

# %%
spark = SparkSession.builder.appName("pyspark-notebook").\
config("spark.jars.packages","com.datastax.spark:spark-cassandra-connector_2.12:3.0.0,com.datastax.spark:spark-cassandra-connector-driver_2.12:3.0.0").\
config("spark.cassandra.connection.host","cassandra").\
config("spark.cassandra.auth.username","cassandra").\
config("spark.cassandra.auth.password","cassandra").\
getOrCreate()

# %%
"""
### Data retrieval from Cassandra
##### A generic method to read data from cassandra. Takes a condition to filter data from a dataframe, a field name to aggregate and a parameter whether to limit some data. Returns Pandas dataframe
"""

# %%
def read_cassandra(filter_condition,group_by,limit=False):
    logs_df = spark\
             .read\
             .format("org.apache.spark.sql.cassandra")\
             .options(table="nasalog", keyspace="loganalysis")\
             .load()\
             .filter(filter_condition)
    agg_df =logs_df.groupBy(group_by).count().sort(group_by)
    if limit:
        return agg_df.limit(5).toPandas()
    else:
        return agg_df.toPandas()

# %%
"""
### Data Retrieval from HDFS
##### A generic method to read data from HDFS. Takes a field name to aggregate data, optional time format in string and an optional boolean filter condition. Returns Pandas Dataframe.
"""

# %%
schema="host string,time string,method string,url string,response string,bytes string"
def unique_hosts(group_by,time_format=None,filter_resp=False):
    logs_df = spark\
             .read\
             .csv("hdfs://namenode:8020/output/nasa_logs/",schema=schema)
    if time_format:
        logs_df = logs_df.withColumn(group_by,date_format(from_unixtime(col("time")),time_format))#.orderBy("time")
    if filter_resp:
        logs_df = logs_df.filter("response==404")
    agg_df =logs_df.limit(80000).groupBy(group_by).count().sort(group_by)
    return agg_df.toPandas()

# %%
"""
### Dash App Creation
##### Creation of Dash Multipage application object and definition of application layout.
"""

# %%
app = dash.Dash(__name__, suppress_callback_exceptions=True)

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
], style={'textAlign': 'center'})

# %%
"""
### Header and Table creation
##### Definition to Header Style and Table generation which has two columns. A header row and data rows.
"""

# %%
#Color assignment
colors = {
    'background': 'white',#'#0C0F0A',
    'text': '#FFFFFF'
}

def create_header(title):
    header_style = {
        'background-color' : '#1B95E0',
        'padding' : '1.5rem',
        'color': 'white',
        'font-family': 'Verdana, Geneva, sans-serif'
    }
    header = html.Header(html.H1(children=title, style=header_style))
    return header

def generate_table(df, max_rows=10):
    table = html.Table(className="responsive-table",
                      children=[
                          html.Thead(
                              html.Tr(
                                  children=[html.Th(col.title()) for col in df.columns.values]
                                  
                                  ),style={'border':'1px black solid'}
                              ),
                          html.Tbody(
                              [
                              html.Tr(
                                  children=[html.Td(data) for data in d]
                                  )
                               for d in df.values.tolist()],style={'border':'1px black solid'})
                          ]
                       , style={'marginLeft': 'auto', 'marginRight': 'auto'}
    )
    
    return table

# %%
"""
### Page creation
##### Index, Real-time, Hourly and Daily dashboard page definitions.
"""

# %%
index_page = html.Div([
    html.Div([create_header('Log Analysis - Dashboard')]),
    dcc.Link('Go to Realtime Dash Board', href='/real-time'),
    html.Br(),
    dcc.Link('Go to Hourly Dash Board', href='/hourly'),
    html.Br(),
    dcc.Link('Go to Daily Dash Board', href='/daily'),
])

realtime_dashboard = html.Div(style={'backgroundColor': colors['background']}, children=
    [   
        html.Div([create_header('Log Analysis - Realtime Dashboard')]),
        html.Div([dcc.Graph(id='live-graph', animate=False)
                 ]
                 ,style={'width': '100%', 'display': 'inline-block'}
                ),
        html.Div([dcc.Graph(id='live-graph1', animate=False)
                 ]
                 ,style={'width': '100%', 'display': 'inline-block'}
                ),
        html.Div([dcc.Graph(id='live-graph2', animate=False)
                 ]
                 ,style={'width': '100%', 'display': 'inline-block'}
                ),
        html.Div([html.H2("Top Paths"), 
                  html.Div(id="top-paths-table")]
                 ,style={'width': '50%', 'display': 'inline-block', 'border':'2px black solid'}
                ),
        ##Intervals define the frequency in which the html element should be updated
        dcc.Interval(id='graph-update',interval=60*1000, n_intervals=0),
        html.Div(id='real-time-content'),
        html.Br(),
        dcc.Link('Go to Hourly Dash Board', href='/hourly'),
        html.Br(),
        dcc.Link('Go to Daily Dash Board', href='/daily'),
        html.Br(),
        dcc.Link('Go back to home', href='/')
    ]
)

hourly_dashboard = html.Div(style={'backgroundColor': colors['background']}, children=
    [   
        html.Div([create_header('Log Analysis - Hourly Dashboard')]),
        html.Div([dcc.Graph(id='hourly-graph', animate=False)
                 ]
                 ,style={'width': '100%', 'display': 'inline-block'}
                ),
        ##Intervals define the frequency in which the html element should be updated
        dcc.Interval(id='hourly-graph-update',interval=60*1000, n_intervals=0),
        html.Div(id='hourly-content'),
        html.Br(),
        dcc.Link('Go to Daily Dash Board', href='/daily'),
        html.Br(),
        dcc.Link('Go to RealTime Dash Board', href='/real-time'),
        html.Br(),
        dcc.Link('Go back to home', href='/')
    ]
)

daily_dashboard = html.Div(style={'backgroundColor': colors['background']}, children=
    [   
        html.Div([create_header('Log Analysis - Daily Dashboard')]),
        html.Div([dcc.Graph(id='daily-graph', animate=False)
                 ]
                 ,style={'width': '100%', 'display': 'inline-block'}
                ),
        html.Div([dcc.Graph(id='daily-graph1', animate=False)
                 ]
                 ,style={'width': '100%', 'display': 'inline-block'}
                ),
        ##Intervals define the frequency in which the html element should be updated
        dcc.Interval(id='daily-graph-update',interval=60*1000, n_intervals=0),
        html.Div(id='daily-content'),
        html.Br(),
        dcc.Link('Go to RealTime Dash Board', href='/real-time'),
        html.Br(),
        dcc.Link('Go to Hourly Dash Board', href='/hourly'),
        html.Br(),
        dcc.Link('Go back to home', href='/')
    ]
)

# %%
"""
### Realtime Dashboard
##### Call back to update dashboard in realtime. Reads data from cassandra database and updates every 60 seconds with latest data. Plots data in Scatter chart, Pie chart, Bar graph and a data Table. this is the hot path in Lambda architecture.
"""

# %%
#Call back for live graph
@app.callback(Output('live-graph', 'figure'),
              Input('graph-update', 'n_intervals')
             )
def update_graph_scatter(n_intervals):
    try:
        processed_time = 0
        filter_condition = "CAST(response AS DECIMAL) IS NOT NULL and time_added >'"+str(processed_time)+"'"
        group_by = 'response'
        df = read_cassandra(filter_condition,group_by)
        processed_time = time.time()-60
        df.dropna(inplace=True)
        
        #Define X and Y axis values        
        X = df["response"]
        Y = df['count']
        
        #Scatter graph definition
        data = go.Scatter(
                x=X,
                y=Y
                )

        return {'data': [data],'layout' : go.Layout(xaxis=dict(range=[X.min(),X.max()],title='Status Codes'),
                                                    yaxis=dict(range=[Y.min(),Y.max()],title='Count'),
                                                    title='Status graphing'
                                                   )
               } 

    except Exception as e:
        #File to capture exceptions
        with open('errors.txt','a') as f:
            f.write(str(e))
            f.write('\n')
            
@app.callback(Output('live-graph1', 'figure'),
              Input('graph-update', 'n_intervals')
             )
def update_graph_scatter1(n_intervals):
    try:
        processed_time = 0
        filter_condition = "CAST(response AS DECIMAL) IS NOT NULL and time_added >'"+str(processed_time)+"'"
        group_by = 'response'
        df = read_cassandra(filter_condition,group_by)
        processed_time = time.time()-60
        df.dropna(inplace=True)
        
        #Define X and Y axis values        
        X = df["response"]
        Y = df['count']
        
        #Scatter graph definition
        data = go.Pie(
                labels=X,
                values=Y
                )

        return {'data': [data]
               } 

    except Exception as e:
        #File to capture exceptions
        with open('errors.txt','a') as f:
            f.write(str(e))
            f.write('\n')

@app.callback(Output('live-graph2', 'figure'),
              Input('graph-update', 'n_intervals')
             )
def update_graph_scatter2(n_intervals):
    try:
        processed_time = 0
        filter_condition = "time_added >'"+str(processed_time)+"'"
        group_by = 'extension'
        df = read_cassandra(filter_condition,group_by)
        processed_time = time.time()-60
        df.dropna(inplace=True)
        
        #Define X and Y axis values        
        X = df["extension"]
        Y = df['count']
        
        #Scatter graph definition
        data = go.Bar(
                x=X,
                y=Y,
            #width=5
                )

        return {'data': [data],'layout' : go.Layout(xaxis=dict(range=[X.min(),X.max()],title='Paths'),
                                                    yaxis=dict(range=[Y.min(),Y.max()],title='Number of Hits'),
                                                    title='Visualizing Paths'
                                                   )
               } 

    except Exception as e:
        #File to capture exceptions
        with open('errors.txt','a') as f:
            f.write(str(e))
            f.write('\n')

@app.callback(Output('top-paths-table', 'children'),
              Input('graph-update', 'n_intervals')
             )
def update_top_urls(n_intervals):
    try:
        processed_time = 0
        filter_condition = "time_added >'"+str(processed_time)+"'"
        group_by = 'url'
        df = read_cassandra(filter_condition,group_by,True)
        processed_time = time.time()-60

        df = df[['url','count']]

        return generate_table(df, max_rows=5)
    except Exception as e:
        #File to capture exceptions
        with open('table_errors.txt','a') as f:
            f.write(str(e))
            f.write('\n')

# Update the index
@app.callback(dash.dependencies.Output('page-content', 'children'),
              [dash.dependencies.Input('url', 'pathname')])
def display_page(pathname):
    if pathname == '/real-time':
        return realtime_dashboard
    elif pathname == '/hourly':
        return hourly_dashboard
    elif pathname == '/daily':
        return daily_dashboard
    else:
        return index_page

# %%
"""
### Hourly Dashboard
##### Call back to update dashboard every hour. Reads data from HDFS and updates every hour with latest data. Plots data as Scatter chart. This is a cold path in Lambda Architecture.
"""

# %%
@app.callback(Output('hourly-graph', 'figure'),
              Input('hourly-graph-update', 'n_intervals')
             )
def update_hourly_scatter(n_intervals):
    try:
        group_by = "hour"
        time_format = 'yy-MM-dd-HH'
        df = unique_hosts(group_by,time_format)
        df.dropna(inplace=True)
        
        #Define X and Y axis values        
        X = df["hour"]
        Y = df['count']
        
        #Scatter graph definition
        data = go.Scatter(
                x=X,
                y=Y,
            #width=5
                    )

        return {'data': [data],'layout' : go.Layout(xaxis=dict(range=[X.min(),X.max()],title='Hour in a day (yy-MM-dd-HH)'),
                                                    yaxis=dict(range=[Y.min(),Y.max()],title='Number of Hits'),
                                                    title='Unique Requests Per Hour'
                                                   )
               } 

    except Exception as e:
        #File to capture exceptions
        with open('errors.txt','a') as f:
            f.write(str(e))
            f.write('\n')

# %%
"""
### Daily Dashboard
##### Call back to update dashboard every day. Reads data from HDFS and updates every hour with latest data. Plots data as Scatter chart and Bar Graph. This is another cold path in Lambda Architecture.
"""

# %%
@app.callback(Output('daily-graph', 'figure'),
              Input('daily-graph-update', 'n_intervals')
             )
def update_daily_scatter(n_intervals):
    try:
        group_by = "day"
        time_format = 'yy-MM-dd'
        df = unique_hosts(group_by,time_format)
        df.dropna(inplace=True)
        
        #Define X and Y axis values        
        X = df["day"]
        Y = df['count']
        
        #Scatter graph definition
        data = go.Scatter(
                x=X,
                y=Y
                )

        return {'data': [data],'layout' : go.Layout(xaxis=dict(range=[X.min(),X.max()],title='Day'),
                                                    yaxis=dict(range=[Y.min(),Y.max()],title='Count'),
                                                    title='Unique Request Per Day'
                                                   )
               } 

    except Exception as e:
        #File to capture exceptions
        with open('errors.txt','a') as f:
            f.write(str(e))
            f.write('\n')
            
@app.callback(Output('daily-graph1', 'figure'),
              Input('daily-graph-update', 'n_intervals')
             )
def update_daily_pie(n_intervals):
    try:
        group_by = "response"
        df = unique_hosts(group_by)
        df.dropna(inplace=True)
        
        #Define X and Y axis values        
        X = df["response"]
        Y = df['count']
        
        #Scatter graph definition
        data = go.Pie(
                labels=X,
                values=Y
                )

        return {'data': [data]
               } 

    except Exception as e:
        #File to capture exceptions
        with open('errors.txt','a') as f:
            f.write(str(e))
            f.write('\n')


# %%
"""
### Main
##### The execution of Dash App starts from here.
"""

# %%
if __name__ == '__main__':
    app.run_server(debug=False, use_reloader=False, port=8050,host= '0.0.0.0')