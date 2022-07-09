# -*- coding: utf-8 -*-
"""
physmonQA.py
Created June 17, 2022
Graphs monthly averages
-Pick the variables to graph via drop down menu
-Pick the time span of the graph (1m,6m,1yr,5yr,max) via radio buttons
@author: dossantoss
"""
# -*- coding: utf-8 -*-
#import plotly dash components
import dash
from dash import dcc
from dash import html
from dash.dependencies import Input, Output
import pandas as pd

#import mysql DB components
import mysql.connector
from mysql.connector import Error

#other import
from datetime import datetime, timedelta
import plotly.graph_objects as go
from plotly.subplots import make_subplots



external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)


################## Database Connection - measure_st - get a list of source tables
try:
    mydb = mysql.connector.connect(host= 'localhost',
                                          database= 'physmon_main',
                                          user= 'root',
                                          password= 'lasper08767')        
    #declare cursor to interact w/DB
    mycursor=mydb.cursor()
    #declare a querry
    myquery = "(SELECT source_table, datum1 from measure_st where writable=1)"
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



#FUNCTION ###############function to build query statement for a physical dat table during a certain time-span
def fdb_query(pastdatet,dfmeasure_st,del_option):
    #convert past datetime to string 
    str_pastdatet = datetime.strftime(pastdatet, '%Y-%m-%d %H:%M:%S')
    
    datum = dfmeasure_st[0][0]
    table = dfmeasure_st[0][1] 
    #print("in db_query")
    #print(datum1)
    #print(table1)
    #print(table2)
    #print(str_pastdatet)
    #print(del_option)
    
    if(del_option ==  'filter-del'):
      myselect = "(SELECT " +table+ ".date_t, " +table+ "." +datum
      myfrom   = " FROM " +table
      mywhere = " WHERE del=0 and date_t>'" +str_pastdatet+ "')"
    else:
      myselect = "(SELECT " +table+ ".date_t, " +table+ "." +datum
      myfrom   = " FROM " +table 
      mywhere = " WHERE date_t>'" +str_pastdatet+ "')"
    
    #Fetch date_t and datum-values from physmon table
    dbquery = myselect + myfrom + mywhere 
    return dbquery   


#FUNCTION ############unction to build SQL statement for a physical dat table for MAX TIME SPAN
def fdb_maxquery(dfmeasure_st, del_option):
    
    datum = dfmeasure_st[0][0]
    table = dfmeasure_st[0][1] 
    #print("in db_query")
    #print(datum)
    #print(table1)
    #print(str_pastdatet)
    
    if(del_option ==  'filter-del'):
        myselect = "(SELECT " +table+ ".date_t, " +table+ "." + datum
        myfrom   = " FROM " +table
        mywhere = " WHERE del=0)"
       
        #Fetch date_t and datum-values from physmon table
        dbquery = myselect + myfrom + mywhere 
    else:
        myselect = "(SELECT " +table+ ".date_t, " +table+ "." + datum
        myfrom   = " FROM " +table + ")" 
        #Fetch date_t and datum-values from physmon table
        dbquery = myselect + myfrom
       
    return dbquery   



#################### STATS ####################################################
#FUNCTION ###############Function to build SQL statement for monthly stats 
def fdb_monthstats(dfmeasure_st,year):
   
    datum = dfmeasure_st[0][0]
    table = dfmeasure_st[0][1]    
    strdatum = str(datum)
    
 
    #Collect monthly stats for year i
    #For rainfall compute totals
    if (strdatum == "ra"):
        #Build the query
        myselect = "(SELECT month(date_t) as month, year(date_t) as year, count(" +datum+ ") as records, sum("+datum+") as monthRa"
        myfrom = " FROM " +table
        mywhere = " WHERE del=0 and year(date_t)= " + str(year)
        mygroup = " GROUP BY month(date_t))"  
    #For everything else averages
    else:      
        #Build the query
        myselect = "(SELECT month(date_t) as month, year(date_t) as year, count(" +datum+ ") as records, avg("+datum+") as monthAvg"
        myfrom = " FROM " +table
        mywhere = " WHERE del=0 and year(date_t)= " + str(year)
        mygroup = " GROUP BY month(date_t))"

    dbquery = myselect + myfrom + mywhere + mygroup
    return dbquery  


#FUNCTION ###############Function to build SQL statement for month stats current yr
def fdb_monthstats_curryear(dfmeasure_st,last_datetime):
    
    datum = dfmeasure_st[0][0]
    table = dfmeasure_st[0][1]    
    strdatum = str(datum)
    
      
    #print(strdatum)
    
    #Collect the current year from datetime
    curr_year = last_datetime.year
    curr_month = last_datetime.month
    
    #print(curr_year)
    #print(curr_month)
    
    #Build the query
    #For rainfall compute totals
    if (strdatum == "ra"):
        myselect = "(SELECT month(date_t) as month, count(" +datum+ ") as records, sum(" +datum+ ") as monthRa"
        myfrom = " FROM " +table
        mywhere = " WHERE del=0 and year(date_t)= " + str(curr_year) + " and month(date_t) < " + str(curr_month)
        mygroup = " GROUP BY month(date_t))"
    #For all other variables       
    else:
        myselect = "(SELECT month(date_t) as month, count(" +datum+ ") as records, avg(" +datum+ ") as monthAvg"
        myfrom = " FROM " +table
        mywhere = " WHERE del=0 and year(date_t)= " + str(curr_year) + " and month(date_t) < " + str(curr_month) 
        mygroup = " GROUP BY month(date_t))"


    dbquery = myselect + myfrom + mywhere + mygroup
    return dbquery  
    

#FUNCTION ###############Function to calculate monthly stats
def fmonth_stats(dfphysmon_stats):
    
    #subset the stats dataframe & removed months w/less than 720 records
    dfphysmon_stats = dfphysmon_stats[dfphysmon_stats['records']>719]
    
    #print("Subset w/more than 719 records")
    #pd.set_option('display.max_rows', None)
    #print(dfphysmon_stats)
    
    
    #subset the physmon_stats DF by month
    dfjan = dfphysmon_stats[dfphysmon_stats['month']==1]
    dffeb = dfphysmon_stats[dfphysmon_stats['month']==2]
    dfmar = dfphysmon_stats[dfphysmon_stats['month']==3]
    dfapr = dfphysmon_stats[dfphysmon_stats['month']==4]
    dfmay = dfphysmon_stats[dfphysmon_stats['month']==5]
    dfjun = dfphysmon_stats[dfphysmon_stats['month']==6]
    dfjul = dfphysmon_stats[dfphysmon_stats['month']==7]
    dfaug = dfphysmon_stats[dfphysmon_stats['month']==8]
    dfsep = dfphysmon_stats[dfphysmon_stats['month']==9]
    dfoct = dfphysmon_stats[dfphysmon_stats['month']==10]
    dfnov = dfphysmon_stats[dfphysmon_stats['month']==11]
    dfdec = dfphysmon_stats[dfphysmon_stats['month']==12]
    
    #For each month type calculate the mean of the month stats
    #i.e. The mean of all Januaries, Februaries ...
    jan_mean = dfjan['month_stats'].mean()
    feb_mean = dffeb['month_stats'].mean()
    mar_mean = dfmar['month_stats'].mean()
    apr_mean = dfapr['month_stats'].mean()
    may_mean = dfmay['month_stats'].mean()
    jun_mean = dfjun['month_stats'].mean()
    jul_mean = dfjul['month_stats'].mean()
    aug_mean = dfaug['month_stats'].mean()
    sep_mean = dfsep['month_stats'].mean()
    oct_mean = dfoct['month_stats'].mean()
    nov_mean = dfnov['month_stats'].mean()
    dec_mean = dfdec['month_stats'].mean()
    
    #Append the means of the monthly statistics to a dataframe
    dflongterm_stats =pd.DataFrame({'month':[1], 'month_stats': [jan_mean]})
    dflongterm_stats = dflongterm_stats.append({'month':2, 'month_stats':feb_mean}, ignore_index=True)
    dflongterm_stats = dflongterm_stats.append({'month':3, 'month_stats':mar_mean}, ignore_index=True)
    dflongterm_stats = dflongterm_stats.append({'month':4, 'month_stats':apr_mean}, ignore_index=True)
    dflongterm_stats = dflongterm_stats.append({'month':5, 'month_stats':may_mean}, ignore_index=True)
    dflongterm_stats = dflongterm_stats.append({'month':6, 'month_stats':jun_mean}, ignore_index=True)
    dflongterm_stats = dflongterm_stats.append({'month':7, 'month_stats':jul_mean}, ignore_index=True)
    dflongterm_stats = dflongterm_stats.append({'month':8, 'month_stats':aug_mean}, ignore_index=True)
    dflongterm_stats = dflongterm_stats.append({'month':9, 'month_stats':sep_mean}, ignore_index=True)
    dflongterm_stats = dflongterm_stats.append({'month':10, 'month_stats':oct_mean},ignore_index=True)
    dflongterm_stats = dflongterm_stats.append({'month':11, 'month_stats':nov_mean},ignore_index=True)
    dflongterm_stats = dflongterm_stats.append({'month':12, 'month_stats':dec_mean},ignore_index=True)
    
    #pd.set_option('display.max_rows', None)
    #print(dflongterm_stats)
    return(dflongterm_stats)
    
    
###############################################################################
####################### Web graphics here....##################################
# 1 drop-down menu window
app.layout = html.Div([            
   html.Div([   
        dcc.Dropdown(
            id='yaxis-series',
            options=[{'label':i,'value':i} for i in physmondb_tables],
            value='metpark_cranetop_srb'
        )
   ],style={'width':'40%'}),

# 5 radio buttons  
   html.Div([html.B(children='Span to display the data series')]),       
   html.Div([          
        dcc.RadioItems(
            id='timespan',
            options=[{'label': i, 'value': i} for i in ['one month', 'six months', 'one year', 'two years', 'five years', 'max']],
            value='one month',
            labelStyle={'display': 'inline-block'}
        ),
        dcc.RadioItems(
            id='del_option',          
            options=[{'label': i, 'value':i} for i in ['filter-del', '  no filter']],
            value='filter-del',
            labelStyle={'display': 'inline-block'}
        )
   ],style={'columnCount':2}),

# Two graphs
   html.Div([html.B(children='Data Series')]),
   dcc.Graph(id='physmon-graph'),
   
   html.Div([html.Br()]),
   html.Div([html.B(children='Monthly Averages (Allways excludes del=1)')]),
   dcc.Graph(id='physmon-monthstats')  
  
])



#callback for graph aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa TIME SERIES     
@app.callback(
    Output('physmon-graph', 'figure'),
    [Input('yaxis-series', 'value'),
     Input('del_option', 'value'),
     Input('timespan', 'value')]
)

#Values passed by the order listed on callback         
def update_figure(yaxis_series, del_option, xaxis_timespan): 

    #create a list
    dfphysmon_list=[]
    
    ################## Database Connection - select ......
    try:
       mydb = mysql.connector.connect(host= 'localhost',
                                          database= 'physmon_main',
                                          user= 'root',
                                          password= 'lasper08767')        
       #declare cursor to interact w/DB
       mycursor=mydb.cursor()
       
       ########################### SERIES ###################################
       #query measure_st for yaxis series
       mydatumquery = "(SELECT datum1, last_date_t from measure_st where source_table = '" + yaxis_series + "')"
       mycursor.execute(mydatumquery)     #execute the cursor/query
       my_strecord = mycursor.fetchone()  #Fetch one record (with datum and last_date_t)        
       my_stlist=list(my_strecord)        #Add the record returned to a list
                      
       #datum for data-series converted to string
       strdatum = str(my_stlist[0]) #the datum
       strtable = yaxis_series      #the main table_name 
       mylastdatet= my_stlist[1]    #the last_date_t   
       #add data-series  attributes to an indexed list
       dfphysmon_list.append([strdatum,strtable])
                 
        
       ################# Select according to timespan 
       #Executes the data query based on the timespan(pastdatet) and a data-frame w/the names of the physical data tables
       if xaxis_timespan == 'one month':
                pastdatet = mylastdatet - timedelta(days = 31)
                mytbquery = fdb_query(pastdatet,dfphysmon_list, del_option) #function call
                    
       elif xaxis_timespan == 'six months':
                pastdatet = mylastdatet - timedelta(days = 180)
                mytbquery = fdb_query(pastdatet,dfphysmon_list, del_option) #function call
                         
       elif xaxis_timespan == 'one year':
                pastdatet = mylastdatet - timedelta(days = 365)
                mytbquery = fdb_query(pastdatet,dfphysmon_list, del_option) #function call
                  
       elif xaxis_timespan == 'two years':
                pastdatet = mylastdatet - timedelta(days = 365*2)
                mytbquery = fdb_query(pastdatet,dfphysmon_list, del_option) #function call
                
       elif xaxis_timespan == 'five years':
                pastdatet = mylastdatet - timedelta(days = 365*5)
                mytbquery = fdb_query(pastdatet,dfphysmon_list, del_option) #function call
                   
       elif xaxis_timespan == 'max':
                #Fetch date_t and datum-values from physmon table
                mytbquery = fdb_maxquery(dfphysmon_list, del_option) #function call
                      
       else: #Fetch 3 months
                pastdatet = mylastdatet - timedelta(days = 90)
                mytbquery = fdb_query(pastdatet,dfphysmon_list, del_option) #function call
                
      
       #execute the cursor for for the data series
       mycursor.execute(mytbquery)
       #Build a data frame with the returned values
       dfphysmon=pd.DataFrame(mycursor.fetchall(), columns=['date_t', strdatum])  
       #The y-series
       my_series=dfphysmon[strdatum]
       my_title=strdatum
      
       
                 
    except mysql.connector.Error as error:
       print("Failed to get record from MySQL table: {}".format(error))
    
    finally:
        if (mydb.is_connected()):
           mycursor.close()
           mydb.close()
           #print("MySQL connection for grapha is closed") 
           
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



#callback for graph bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb Monthly Averages     
@app.callback(
    Output('physmon-monthstats', 'figure'),
    [Input('yaxis-series', 'value')]
)

#Values passed by the order listed on callback         
def update_figure(yaxis_series): 

    #create a list
    dfphysmon_list=[]
    
    ################## Database Connection - select ......
    try:
       mydb = mysql.connector.connect(host= 'localhost',
                                          database= 'physmon_main',
                                          user= 'root',
                                          password= 'lasper08767')        
       #declare cursor to interact w/DB
       mycursor=mydb.cursor()
              
       #query measure_st for yaxis-series
       mydatumquery="(SELECT datum1, first_date_t, last_date_t from measure_st where source_table = '" + yaxis_series + "')"
       mycursor.execute(mydatumquery)     #execute the cursor/query
       my_strecord = mycursor.fetchone()  #Fetch one record (with datum and last_date_t)        
       my_stlist=list(my_strecord)        #Add the record returned to a list
                      
       #datum for data-series converted to string
       strdatum = str(my_stlist[0]) #the datum
       strtable = yaxis_series      #the main table_name 
       myfirstdatet = my_stlist[1]    #the first_date_t
       mylastdatet  = my_stlist[2]    #the last_date_t   
       #add data-series  attributes to an indexed list
       dfphysmon_list.append([strdatum,strtable])
       
       #Declare a dataframe to store physmon summary
       dfphysmon_stats = pd.DataFrame()
       
       ########### MONTH STATS ALL PREVIOUS YEARS ######################
       for year in range (myfirstdatet.year, mylastdatet.year-1 ):
                     
           ##Select all the data from the time-series to generate monthly stats
           #Build a query w/ 1)dataframe containing datum_name & table_name and 2)year
           ################# FUNCTION CALL HERE###########
           mytbquery = fdb_monthstats(dfphysmon_list,year)
           ###############################################     
          
           #execute the cursor for the query built on above line function call
           mycursor.execute(mytbquery)
           #Build a data frame with the returned values
           dftemp=pd.DataFrame(mycursor.fetchall(), columns=['month', 'year', 'records', 'month_stats'])  
           dfphysmon_stats = dfphysmon_stats.append(dftemp, ignore_index=True)
                             
       print('\n')
       #pd.set_option('display.max_rows', None)
       #print(dfphysmon_stats)
       
       ### FUNCTION CALL HERE - Generate long monthly stats ####
       dflterm_monthstats = fmonth_stats(dfphysmon_stats)
       #########################################################
       
       
        
       #### MONTH STATS CURRENT YEAR ############################
       ##Select all the data from the time-series to generate monthly stats
       #Build a query w/, 1)dataframe containing datum_name & table_name, and 2) last date_t 
       #### FUNCTION CALL HERE
       curryr_tbquery = fdb_monthstats_curryear(dfphysmon_list, mylastdatet) 

       #execute the cursor for the query built on previous line
       mycursor.execute(curryr_tbquery)
       
       #Build a data frame with the returned values
       #For rainfall compute totals
       if (strdatum == "ra"):
           dfphysmon_curr=pd.DataFrame(mycursor.fetchall(), columns=['month', 'records', 'monthRa'])     
      #For other variables monthly average
       else: 
           dfphysmon_curr=pd.DataFrame(mycursor.fetchall(), columns=['month', 'records', 'monthAvg']) 
             
      #The y-series
       #my_series=dfphysmon_prev['monthAvg']
       #my_month=dfphysmon_prev['month']
       #print("Prev year")
       #print(dfphysmon_prev)
       #print("Curr year")
       #print(dfphysmon_curr)
                
    except mysql.connector.Error as error:
       print("Failed to get record from MySQL table: {}".format(error))
    
    finally:
        if (mydb.is_connected()):
           mycursor.close()
           mydb.close()
           #print("MySQL connection for grapha is closed") 
           
    
    #Graph values returned to callback   
    fig = make_subplots(specs=[[{"secondary_y": False}]])
     
    if(strdatum == "ra"): #For graphs w/totals
       fig.add_trace(
           go.Scatter(x=dflterm_monthstats['month'], y=dflterm_monthstats['month_stats'], name = yaxis_series + " " + str(myfirstdatet.year)+"-"+str(mylastdatet.year-1)),
           secondary_y=False,
       )
     
       fig.add_trace(
           go.Scatter(x=dfphysmon_curr['month'], y=dfphysmon_curr['monthRa'], name = yaxis_series + " " + str(mylastdatet.year)),
           secondary_y=False,    
       )
    else: #For graphs w/No totals
      fig.add_trace(
          go.Scatter(x=dflterm_monthstats['month'], y=dflterm_monthstats['month_stats'], name = yaxis_series + " " + str(myfirstdatet.year)+"-"+str(mylastdatet.year-1)),
          secondary_y=False,
      )
    
      fig.add_trace(
          go.Scatter(x=dfphysmon_curr['month'], y=dfphysmon_curr['monthAvg'], name = yaxis_series + " " + str(mylastdatet.year)),
          secondary_y=False,    
      )
 
    
    fig.update_layout(
        yaxis=dict(
            title=strdatum),
        xaxis=dict(
            tickmode = 'linear',
            tick0 = 1,
            dtick = 1)
    )
   
      
    #Graph values returned to callback
    return fig
    
    
    


if __name__ == '__main__':
    app.run_server(debug=True)