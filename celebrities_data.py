# -*- coding: utf-8 -*-
"""
Created on Fri Apr 24 23:18:59 2020

@author: Rohan Dixit
"""

import requests
import pandas as pd
from IPython.core.display import HTML
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
import re
import pymysql

url = 'https://www.imdb.com/list/ls020280202/'
data = requests.get(url)
Bs  = BeautifulSoup(data.text ,  'html.parser')

Actor_Name = []
for div in Bs.findAll('div'  , attrs = {'class' : 'lister-item-content'}):
    Actor_Name.append(div.find('a').contents[0])
    
Actor_Images = []
for image in Bs.findAll('img' , {'src':re.compile('.jpg')}):
    Actor_Images.append(str(image['src']))




url = 'https://www.imdb.com/list/ls069887650/'
data = requests.get(url)
Bs1 = BeautifulSoup(data.text ,  'html.parser')

Actress_Name = []
for div in Bs1.findAll('div'  , attrs = {'class' : 'lister-item-content'}):
    Actress_Name.append(div.find('a').contents[0])
    
Actress_Images=[]
for image in Bs1.findAll('img' , {'src':re.compile('.jpg')}):
        Actress_Images.append(str(image['src']))

        
Actors=[]   
for i in Actor_Name:
    Actors.append(i.replace('\n' , '').strip())
    
Actress=[]   
for i in Actress_Name:
    Actress.append(i.replace('\n' , '').strip())
    
Celebrities_Names = Actors + Actress  
Celebrities_Images = Actor_Images + Actress_Images 

df = pd.DataFrame()
Celebrities_Information = [] 

for i in Celebrities_Names:

    info_url = "https://en.wikipedia.org/wiki/{}".format(i)
    info_data = requests.get(info_url)
    soup_info = BeautifulSoup(info_data.text , 'html.parser')
    text = ''
    i=0
    for para in soup_info.find_all('p'):
    
        if len(para.text)>100: 
    
            if len(text)< 1000:
        
                text +=(para.text)
            if len(text)> 1000:
                break

    text = text.replace('\n' , '')
    Celebrities_Information.append(text)
df['Image'] = Celebrities_Images
df['Name']=Celebrities_Names
df['Personality Traits']=Celebrities_Information


# For CSV Dataset
df.to_csv('celebrities_info.csv',index=False)

# For SQL Dataset

tableName   = "Celebrities_Information"
dataFrame   = pd.DataFrame(data=df)           
sqlEngine  = create_engine('mysql+pymysql://root:@127.0.0.1/celebrities_information', pool_recycle=3600)
dbConnection = sqlEngine.connect()
try:
    frame = dataFrame.to_sql(tableName, dbConnection, if_exists='fail')
except ValueError as vx:

    print(vx)

except Exception as ex:   

    print(ex)

else:

    print("Table %s created successfully."%tableName);   

finally:

    dbConnection.close()


