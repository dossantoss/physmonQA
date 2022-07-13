# -*- coding: utf-8 -*-
"""
Spyder Editor
Script to update last datetime on measure_st
"""

#library imports
import mysql.connector
import pandas as pd





########## READ DATA FROM MEASURE_ST
##Database Connection-measure_st - get a list of source tables
try:
    mydb = mysql.connector.connect(host= 'localhost',
                                          database= 'physmon_data',
                                          user= 'root',
                                          password= 'lasper08767')        
    #declare cursor to interact w/DB
    mycursor=mydb.cursor()
    #declare a querry
    myquery = "(SELECT source_table, datum1, last_date_t from measure_st)"
    #execute the cursor
    mycursor.execute(myquery)
    #Fetch records and covert into a data frame (df)
    df=pd.DataFrame(mycursor.fetchall(), columns=['source_table','datum1', 'last_date_t'])
    
except mysql.connector.Error as error:
    print("Failed to get record from MySQL table: {}".format(error))
    
finally:
    if (mydb.is_connected()):
        mycursor.close()
        mydb.close()
        #print("MySQL connection is closed")


############ READ Physical data tables last date_t
############ Synchronize last_date_t on mst w/last date_t on tables
for i in df.index:
    
    try:
        mydb = mysql.connector.connect(host= 'localhost',
                                              database= 'physmon_data',
                                              user= 'root',
                                              password= 'lasper08767')        
        #declare cursor to interact w/DB
        mycursor=mydb.cursor()
        #declare a querry
        myquery = ("SELECT date_t from " + df['source_table'][i] + " order by date_t desc limit 1")
        #execute the cursor
        mycursor.execute(myquery)
        #Fetch records
        dftime= pd.DataFrame(mycursor.fetchone(), columns=['date_t'])
        
        
        print("\n")
        print("Measure_st status: ", df['source_table'][i], df['last_date_t'][i])
        print("Last date_t on tb: ", df['source_table'][i], dftime['date_t'][0] )
    
        if (df['last_date_t'][i] != dftime['date_t'][0]) :
            
            print ("Difference between last datetime on measure_st and table last datetime")
            print ("Updating measure_st ....")
            #Update measure_st
            myupdate = ("update measure_st set last_date_t = '" + str(dftime['date_t'][0]) + "'")
            mywhere = (" where source_table = '" + str(df['source_table'][i]) + "'")
            newquery = myupdate + mywhere                
            #print(newquery)
            mycursor.execute(newquery)
            mydb.commit()
     
    except mysql.connector.Error as error:
        print("Failed to get record from MySQL table: {}".format(error))
        
    finally:
        if (mydb.is_connected()):
            mycursor.close()
            mydb.close()
            #print("MySQL connection is closed")