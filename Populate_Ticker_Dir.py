import time
import sqlalchemy as db
import pandas as pd
import os
import glob
import multiprocessing
import numpy as np
import sqlalchemy.orm as sao
import mysql
import mysql.connector
import itertools

connection = mysql.connector.connect(host='localhost',
                                 port = 3306,
                                 database='dir',
                                 user='akanksha',
                                 password='password')
cursor = connection.cursor()
def process_(file, ct,L1,L2,L3,L4,L5,L6,L7,L8,L9,L10):
    filename = os.path.splitext(file)[0]
    x = filename[0]

    ticker_dict = {"dir.tl_1": L1,
               "dir.tl_2": L2,
               "dir.tl_3": L3,
               "dir.tl_4": L4,
               "dir.tl_5": L5,
               "dir.tl_6": L6,
               "dir.tl_7": L7,
               "dir.tl_8": L8,
               "dir.tl_9": L9,
               "dir.tl_10": L10
    }

    tbl = str([k for k,v in ticker_dict.items() if x in v][0])
    sql_query = 'INSERT INTO ' + tbl + ' (Stock_ID,Symbol) VALUES (\'' + str(ct) + '\',\'' + filename + '\')'
    cursor.execute(sql_query)
    print('Inserted symbol for ', filename)
    connection.commit()
#------------------------------------------------------------------------
if __name__ == "__main__":
    os.chdir('C:\\Users\Dell\Desktop\Google Drive')
    start_time = time.time()
#-------------------------------------------------------------------------
    l_dict = {"A":0,"B":0,"C":0,"D":0,"E":0,"F":0,"G":0,"H":0,"I":0,"J":0,"K":0,"L":0,"M":0,"N":0,"O":0,"P":0,"Q":0,"R":0,"S":0,"T":0,"U":0,"V":0,"W":0,"X":0,"Y":0,"Z":0}
    for file in glob.glob('*.csv'):
        filename = os.path.splitext(file)[0]
        x = filename[0]
        l_dict[x] = l_dict[x] + 1
    l_dict = sorted(l_dict.items(), key=lambda x: x[1], reverse=True)
    d = dict(l_dict)
    d1 = dict(itertools.islice(d.items(), 1))
    d2 = dict(itertools.islice(d.items(),1,2))
    d3 = dict(itertools.islice(d.items(),2,3))
    d4 = dict(itertools.islice(d.items(),3,4))
    d5 = dict(itertools.islice(d.items(),4,5))
    d6 = dict(itertools.islice(d.items(),5,6))
    d7 = dict(itertools.islice(d.items(),6,7))
    d8 = dict(itertools.islice(d.items(),7,8))
    d9 = dict(itertools.islice(d.items(),8,9))
    d10 = dict(itertools.islice(d.items(),9,10))
    
    for i in range(10,26):
        x = min([sum(d1.values()),sum(d2.values()),sum(d3.values()),sum(d4.values()),sum(d5.values()),sum(d6.values()),sum(d7.values()),sum(d8.values()),sum(d9.values()),sum(d10.values())])
        if x == sum(d1.values()):
            key = list(d.items())[i][0]
            d1[key] = list(d.items())[i][1]
            
        elif x == sum(d2.values()): 
            key = list(d.items())[i][0]
            d2[key] = list(d.items())[i][1]
            
        elif x == sum(d3.values()):
            key = list(d.items())[i][0]
            d3[key] = list(d.items())[i][1]
            
        elif x == sum(d4.values()): 
            key = list(d.items())[i][0]
            d4[key] = list(d.items())[i][1]
            
        elif x == sum(d5.values()): 
            key = list(d.items())[i][0]
            d5[key] = list(d.items())[i][1]
            
        elif x == sum(d6.values()): 
            key = list(d.items())[i][0]
            d6[key] = list(d.items())[i][1]
            
        elif x == sum(d7.values()):
            key = list(d.items())[i][0]
            d7[key] = list(d.items())[i][1]
            
        elif x == sum(d8.values()):
            key = list(d.items())[i][0]
            d8[key] = list(d.items())[i][1]

        elif x == sum(d9.values()):
            key = list(d.items())[i][0]
            d9[key] = list(d.items())[i][1]

        elif x == sum(d10.values()):
            key = list(d.items())[i][0]
            d10[key] = list(d.items())[i][1]

    L1 = [k  for  k in d1.keys()]
    L2 = [k  for  k in d2.keys()]
    L3 = [k  for  k in d3.keys()]
    L4 = [k  for  k in d4.keys()]
    L5 = [k  for  k in d5.keys()]
    L6 = [k  for  k in d6.keys()]
    L7 = [k  for  k in d7.keys()]
    L8 = [k  for  k in d8.keys()]
    L9 = [k  for  k in  d9.keys()]
    L10 = [k  for  k in  d10.keys()]
    count = 1
    for file in glob.glob('*.csv'):
        process_(file,count,L1,L2,L3,L4,L5,L6,L7,L8,L9,L10)
        count = count + 1
#--------------------------------------------------------------------------
    print("My program took", time.time() - start_time, " seconds to run")