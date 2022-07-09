# -*- coding: utf-8 -*-
"""
physmon_QAstats.py
Created on  July 8, 2022
WORKS on physmon_data (RAW) DATABASE
PRODUCES DB VIEW W/ SERIES STATISTICS
-Creates a stats table, a time series graph and histogram of a time series
-The user can pick the variables to graph via drop down menu
-The user can pick the time span of the graph (1m,6m,1yr,5yr,max) via radio buttons
@author: dossantoss
"""
# -*- coding: utf-8 -*-
#import plotly dash components
import dash
from dash import dcc
from dash import html
from dash.dependencies import Input, Output
import pandas as pd
import numpy as np


#import mysql DB components
import mysql.connector
from mysql.connector import Error


#other import
from datetime import datetime, timedelta

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)


###Database Connection-measure_st - get a list of source tables
try:
    mydb = mysql.connector.connect(host= 'localhost',
                                          database= 'physmon_data',
                                          user= 'root',
                                          password= 'lasper08767')        
    #declare cursor to interact w/DB
    mycursor=mydb.cursor()
    #declare a querry
    myquery = "(SELECT source_table, datum1 from measure_st)"
    #execute the cursor
    mycursor.execute(myquery)
    #Fetch records and covert into a data frame (df)
    df=pd.DataFrame(mycursor.fetchall(), columns=['source_table','datum1'])
    
except mysql.connector.Error as error:
    print("Failed to get record from MySQL table: {}".format(error))
    
finally:
    if (mydb.is_connected()):
        mycursor.close()
        mydb.close()
        #print("MySQL connection is closed")

#Build a list of source tables
physmondb_tables = df['source_table'].unique()

#Function to calculate percentile stats
def pct_function(datalist):
    
      mini = np.min(datalist)
      maxi = np.max(datalist)
      pct90 = np.percentile(datalist,90)
      pct10 = np.percentile(datalist,10)
      pct25 = np.percentile(datalist,25)
      pct75 = np.percentile(datalist,75)
      median = np.median(datalist)
      return(mini, maxi, pct10, pct25, pct75, pct90, median)


####################### Web graphics here....
# 2 drop-down menu windows and 5 radio options
app.layout = html.Div([            
   html.Div([
              html.P(html.B("OPERATES ON physmon_data (RAW) DB",style={'color': 'red', 'fontSize': 10})),
              dcc.Dropdown(
                    id='yaxis-series',
                    options=[{'label':i,'value':i} for i in physmondb_tables],
                    value='metpark_crane25m_at'
                    ),
              
                 html.Div('Filter DEL:', style={'color': 'blue', 'fontSize': 12}),
                 dcc.RadioItems(
                    id='filter-del',
                    options=[{'label': i, 'value': i} for i in ['filter del', 'no filter']],
                    value='filter del',
                    labelStyle={'display': 'inline-block'}
                    ),
              
                 html.Div('Calculate Stats for QA routines:', style={'color': 'blue', 'fontSize': 12}),
                 dcc.RadioItems(
                    id='QA',
                    options=[{'label': i, 'value': i} for i in ['range', 'rolling median']],
                    value='range',
                    labelStyle={'display': 'inline-block'}
                    )
   ],style={'width': '50%', 'display': 'inline-block'}), 
   html.P(html.B("For Range:Annotate DB with RANGE fail (2) for values outside the L90PCT_fence and H90PCT_fence")),
   html.P(html.B("For Rolling Median:Annotate DB with STEP fail for values with rmdiff > H90PCT_fence")),
   html.Table([
        html.Tr([html.Td('Min'),     html.Td('Max'),     html.Td('10th pct'), html.Td('25th pct'), html.Td('75th pct'), html.Td('90th pct'), html.Td('Median'),   html.Td('LIQR_fence'), html.Td('HIQR_fence'), html.Td(html.B('L90PCT_fence')), html.Td(html.B('H90PCT_fence'))]),  
        html.Tr([html.Td(id='mini'), html.Td(id='maxi'), html.Td(id='pct10'), html.Td(id='pct25'), html.Td(id='pct75'), html.Td(id='pct90'), html.Td(id='median'),html.Td(id='LIQR'),    html.Td(id='HIQR'),    html.Td(id='LPCT'),      html.Td(id='HPCT')   ]),
    ]),
   dcc.Graph(id='physmon-graph'),
   dcc.Graph(id='physmon-distribution')  
])


#Callback for statistics tab
@app.callback(
    [Output('mini', 'children'),
     Output('maxi', 'children'),
     Output('pct10', 'children'),
     Output('pct25', 'children'),
     Output('pct75', 'children'),
     Output('pct90', 'children'),
     Output('median','children'),
     Output('LIQR',  'children'),
     Output('HIQR',  'children'),
     Output('LPCT',  'children'),
     Output('HPCT',  'children')],
   [Input('yaxis-series', 'value'),
    Input('QA', 'value'),
    Input('filter-del', 'value')])

def render_content(yaxis_series, qa_routine, qa_filter):   
   ################## Database Connection - select ......
   try:
        mydb = mysql.connector.connect(host= 'localhost',
                                          database= 'physmon_data',
                                          user= 'root',
                                          password= 'lasper08767')        
        #declare cursor to interact w/DB
        mycursor=mydb.cursor()
       
        ###### QUERY THE SERIES TABLE: measure_st ######
        #query measure_st for datum and last_date_t
        mydatumquery = "(SELECT datum1, last_date_t from measure_st where source_table = '" + yaxis_series + "')"
        mycursor.execute(mydatumquery)   #execute the cursor/query
        mymeasurst = mycursor.fetchone()    #Fetch one record (with datum and last_date_t)        
        my_datumlist=list(mymeasurst)       #Add the record returned to a list
                      
        #datum and source_table name converted to string
        strdatum = str(my_datumlist[0]) #the datum
        strtable = str(yaxis_series)    #the table_name for to graph   
             
        ########## Calculate Rolling Median STATS ############
        if qa_routine=='rolling median':
                #Act on filtering DEL or not
                if qa_filter == 'filter del':
                    mytbquery = "(SELECT rmdiff from " + strtable + " where del!=1)"
                    mycursor.execute(mytbquery)
                else: 
                    mytbquery = "(SELECT rmdiff  from " + strtable + ")"
                    mycursor.execute(mytbquery)
                                    
                #rmdiff values placed inside a dataframe
                dfrm=pd.DataFrame(mycursor.fetchall(), columns=["rmdiff"])
                fseries = dfrm["rmdiff"].astype(float)
                #########Call pct_function to calculates statistics###########    
                (mini, maxi, pct10, pct25, pct75, pct90, median) = pct_function(fseries)
                 
        else: 
                ########## Calculate RANGE STATS ############
                if qa_filter == 'filter del':   
                   mytbquery = "(SELECT " + strdatum + " from " + strtable + " where del!=1)"
                   mycursor.execute(mytbquery)
                else:
                   mytbquery = "(SELECT " + strdatum + " from " + strtable + ")"
                   mycursor.execute(mytbquery)
                   
                #series values placed inside a dataframe
                dfdatum=pd.DataFrame(mycursor.fetchall(), columns=[strdatum])
                fseries = dfdatum[strdatum].astype(float)                
                #########Call pct_function to calculates statistics###########
                (mini, maxi, pct10, pct25, pct75, pct90, median) = pct_function(fseries) 
                
        #IQR fences
        LIQR = pct25 - (1.5 * abs(pct75-pct25))
        HIQR = pct75 + (1.5 * abs(pct75-pct25))
        #PCT fences
        LPCT= median - (3*abs(pct90 - median))
        HPCT= median + (3*abs(pct90 - median))               
        #round some stuff
        LIQR = round(LIQR,3)
        HIQR = round(HIQR,3)
        LPCT = round(LPCT,3)
        HPCT = round(HPCT,3)
                
                
   except mysql.connector.Error as error:
       print("Failed to get record from MySQL table: {}".format(error))
    
   finally:
        if (mydb.is_connected()):
           mycursor.close()
           mydb.close()
           print("MySQL connection for stats closed")               

   return mini, maxi, pct10, pct25, pct75, pct90, median, LIQR, HIQR, LPCT, HPCT


#callback for graph - aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa       
@app.callback(
    Output('physmon-graph', 'figure'),
    [Input('yaxis-series', 'value'),
     Input('QA', 'value'),
     Input('filter-del', 'value')])

#Values passed by the order listed on callback         
def update_figure(yaxis_series, qa_routine, qa_filter): 

    ################## Database Connection - select ......
    try:
       mydb = mysql.connector.connect(host= 'localhost',
                                          database= 'physmon_data',
                                          user= 'root',
                                          password= 'lasper08767')        
       #declare cursor to interact w/DB
       mycursor=mydb.cursor()
       
       ###### QUERY THE SERIES TABLE: measure_st ######
       #query measure_st for datum and last_date_t
       mydatumquery = "(SELECT datum1, last_date_t from measure_st where source_table = '" + yaxis_series + "')"
       mycursor.execute(mydatumquery)   #execute the cursor/query
       mymst = mycursor.fetchone()    #Fetch one record (with datum and last_date_t)        
       my_datumlist=list(mymst)       #Add the record returned to a list
                      
       #datum and source_table name converted to string
       strdatum = str(my_datumlist[0]) #the datum
       strtable = str(yaxis_series)    #the table_name for to graph   
        
       
       ######### Select rmdiff data to graph ############
       if qa_routine=='rolling median':
          mytbquery = "(SELECT date_t, rmdiff from " + strtable + " where del!=1)"
          mycursor.execute(mytbquery)
          #Build a data frame with the returned values
          dfphysmon=pd.DataFrame(mycursor.fetchall(), columns=['date_t', 'rmdiff']) 
          #The y-axis series
          my_series=dfphysmon['rmdiff']
          my_title='rmdiff'
            
       else:            
       ######### Select data series values 
            if qa_filter == 'filter del':
                 mytbquery = "(SELECT date_t, " + strdatum + " from " + strtable + " where del!=1)"               
            else:
                mytbquery = "(SELECT date_t, " + strdatum + " from " + strtable + ")"
          
            #execute the cursor for select data series option
            mycursor.execute(mytbquery)
            #Build a data frame with the returned values
            dfphysmon=pd.DataFrame(mycursor.fetchall(), columns=['date_t', strdatum])  
            #The y-axis series
            my_series=dfphysmon[strdatum]
            my_title=strdatum
            #print(dfphysmon)
    
    except mysql.connector.Error as error:
       print("Failed to get record from MySQL table: {}".format(error))
    
    finally:
        if (mydb.is_connected()):
           mycursor.close()
           mydb.close()
           print("MySQL connection for distribution graph closed")    
           
    #Graph values returned to callback
    return {
        'data':[dict(              
                x=dfphysmon['date_t'],
                y=my_series,
                name=yaxis_series,
                mode='lines'
                )],
         'layout': dict(
                   yaxis={
                          'title':my_title,
                          'type': 'linear'
                   },
                    margin={'l':50, 'b':20, 't':20, 'r':20},
                    height=200,
                    showlegend=True,
                    hovermode='closest'
            )
         }


#callback for graphb - DISTRIBUTION    
@app.callback(
    Output('physmon-distribution', 'figure'),
    [Input('yaxis-series', 'value'),
     Input('QA', 'value'),
     Input('filter-del', 'value')])

#Values passed by the order listed on callback         
def update_figure(yaxis_series, qa_routine, qa_filter): 
        
    ################## Database Connection - select ......
    try:
       mydb = mysql.connector.connect(host= 'localhost',
                                          database= 'physmon_data',
                                          user= 'root',
                                          password= 'lasper08767')        
       #declare cursor to interact w/DB
       mycursor=mydb.cursor()
       
       #query measure_st for yaxis_dist series
       mydatumquery = "(SELECT datum1, last_date_t from measure_st where source_table = '" + yaxis_series + "')"
       mycursor.execute(mydatumquery)     #execute the cursor/query
       my_strecord = mycursor.fetchone()  #Fetch one record (with datum and last_date_t)        
       my_stlist=list(my_strecord)        #Add the record returned to a list
                      
       #datum converted to string
       strdatum = str(my_stlist[0])   #the datum
       strtable = str(yaxis_series)   #the main table_name for bottom graph     
       

        ######### Select rolling median data to graph ############
       if qa_routine=='rolling median':
          mytbquery = "(SELECT rmdiff from " + strtable + " )"
          mycursor.execute(mytbquery)
          #Build a data frame with the returned values
          dfphysmon=pd.DataFrame(mycursor.fetchall(), columns=['rmdiff']) 
          #The y-axis series
          my_series = dfphysmon['rmdiff'].astype(float)
          my_title = 'rmdiff'
          ######### BUILD HISTOGRAM #################
          myhist, bin_edges= np.histogram(my_series,100)
            
       else:            
       ######### Select data series values from physmon table                 
           if qa_filter == 'filter del':
                mytbquery = "(SELECT " + strdatum + " from " + strtable + " where del!=1)"  #function call
           else:
                mytbquery = "(SELECT " + strdatum + " from " + strtable + ")"         
 
           #execute the cursor to query the physical DB
           mycursor.execute(mytbquery)
           #Build a data frame with the returned values
           dfphysmon=pd.DataFrame(mycursor.fetchall(), columns=[strdatum])
           my_series = dfphysmon[strdatum].astype(float)
           my_title = strdatum
           ######### BUILD HISTOGRAM #################
           myhist, bin_edges= np.histogram(my_series,100)
           #print(myhist)
           #print(bin_edges)
    
    except mysql.connector.Error as error:
       print("Failed to get record from MySQL table: {}".format(error))
    
    finally:
        if (mydb.is_connected()):
           mycursor.close()
           mydb.close()
           print("MySQL connection for distribution graph closed")    
           
    #Graph values returned to callback
    return {
        'data':[dict(
                x=bin_edges,              
                y=myhist,
                type='bar',
                name=yaxis_series
                )],
         'layout': dict(
                   yaxis={
                          'title':my_title,
                          'type': 'linear'
                   },
                    margin={'l':50, 'b':20, 't':20, 'r':20},
                    height=200,
                    showlegend=True,
                    hovermode='closest'
            )
         }


if __name__ == '__main__':
    app.run_server(debug=True)