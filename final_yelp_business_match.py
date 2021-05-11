#!/usr/bin/env python
# coding: utf-8
# In[37]:


import os
import json
import pandas as pd
import numpy as np
import mysql.connector
import requests


# In[38]:


##build connection to database
##make sure create clients, restaurants and result table in sql database first
user_name = 'root'
password = '13486986677Lly'

cnx = mysql.connector.connect(user=user_name, password=password,
                              host='localhost',
                              database='yelp',auth_plugin='mysql_native_password',port=3306) 


# In[39]:


clients = pd.read_sql('SELECT * FROM clients', con=cnx )
clients


# In[40]:


##define client input function
cursor = cnx.cursor()
def client_to_db(res):
    name = res['name']
    address1 = res['address1']
    city = res['city']
    state = res['state']
    country = res['country']
    if name not in clients['name'].tolist():
       cursor.execute('INSERT INTO clients (name,address1,city,state,country) VALUES (%s,%s,%s,%s,%s)',(name,address1,city,state,country))
    cnx.commit()


# In[41]:


##put clients information into database
res1={'name':"Gary Danko",'address1':"800 N Point St",'city':"San Francisco",'state':"CA",'country':"US"}
res2={'name':"Good Grub Vending",'address1':"758 N Point St",'city':"San Francisco",'state':"CA",'country':"US"}
res3={'name':"Four Barrel Coffee",'address1':"375 Valencia St",'city':"San Francisco",'state':"CA",'country':"US"}
res4={'name':"BLT Steak",'address1':"1625 I St NW",'city':"Washington, DC",'state':"CA",'country':"US"}
client_to_db(res1)
client_to_db(res2)
client_to_db(res3)
client_to_db(res4)


# In[42]:


clients = pd.read_sql('SELECT * FROM clients', con=cnx )
clients


# In[43]:


##define functions for restaurants match
class Yelpmatch:

    def __init__(self,api_key,name,address1,city,state,country):
        self.api_key = api_key
        self.name = name
        self.address1 = address1
        self.city = city
        self.state = state
        self.country = country
        self.business_match_data = None

    def get_business_match_data(self):
        url = 'https://api.yelp.com/v3/businesses/matches'
        headers = {'Authorization':'Bearer %s' % self.api_key}
        params = {'name':self.name,'address1':self.address1,"city":self.city,"state":self.state,"country":self.country}
        json_url = requests.get(url,headers=headers,params=params)
        data = json.loads(json_url.text)
        self.business_match_data=data
        return data

    def dump(self,file_name):
        if self.business_match_data is None:
             return
        with open(file_name,'w') as f:
                json.dump(self.business_match_data, f, indent = 4)


# In[44]:


##check clients data in db
clients = pd.read_sql('SELECT * FROM clients', con=cnx )
clients


# In[45]:


##connect to api and get business match json 
API_KEY = "s8u0op_gSPwpXqfYLuLGxMfFIKPZy3vSYv-87f5TN18IAPMW0YXc4X9bKwn4v7pO3t_YVewHTukZ_X0Ej5pJu1E5JQRS8fs8TkrYNuAXaYfuhKzmnws03yS9kC5jYHYx"
buz_data=[]
for i in range(len(clients)):
    name = clients['name'][i]
    address1 = clients['address1'][i]
    city = clients['city'][i]
    state = clients['state'][i]
    country = clients['country'][i]
    yp = Yelpmatch(API_KEY, name, address1,city,state,country)
    yp.get_business_match_data()
    file_name = 'yelp_business' + str(i) +'.json'
    yp.dump(file_name)


# In[46]:


##combine multiple json files
all_buz=[]
for i in range(len(clients)):
    with open('yelp_business'+ str(i) +'.json') as f:
        data = json.load(f)
    all_buz.append(data['businesses'][0])
all_buz_dic={"businesses":all_buz}


# In[47]:


restaurants = pd.read_sql('SELECT * FROM restaurants', con=cnx )
restaurants


# In[48]:


##store business match results into database
for i, item in enumerate(all_buz_dic['businesses']):
    id = item['id']
    name = item['name']
    city = item['location']['city']
    zip_code = item['location']['zip_code']
    phone= item['phone']
    if id not in restaurants['id'].tolist():
       cursor.execute('INSERT INTO restaurants (id,name,city,zip_code,phone) VALUES (%s,%s,%s,%s,%s)',(id,name,city,zip_code,phone))
       cnx.commit()    


# In[49]:


restaurants = pd.read_sql('SELECT * FROM restaurants', con=cnx )
restaurants


# In[50]:


##define models
def amount(zp):
    if zp[0]=='9':
        return 5000
    else:
        return 3000
def term(zp):
    if zp[0]=='9':
        return 6
    else:
        return 12
def rate(zp):
    if zp[0]=='9':
        return 9
    else:
        return 8


# In[51]:


##model results
restaurants.loc[:,'Amount']=[amount(zp) for zp in restaurants['zip_code']]
restaurants.loc[:,'Term']=[term(zp) for zp in restaurants['zip_code']]
restaurants.loc[:,'Rate']=[rate(zp) for zp in restaurants['zip_code']]


# In[52]:


restaurants


# In[53]:


results = pd.read_sql('SELECT * FROM result', con=cnx )
results


# In[54]:


####store model results into database
for i in range(len(restaurants)):
    id = str(restaurants['id'][i])      
    name = str(restaurants['name'][i])      
    amount = int(restaurants['Amount'][i])
    term = int(restaurants['Term'][i])
    rate = int(restaurants['Rate'][i])
    if id not in results['id'].tolist():
       cursor.execute('INSERT INTO result (id,name,amount,term,rate) VALUES (%s,%s,%s,%s,%s)',(id, name,amount,term,rate))
       cnx.commit()


# In[55]:


results = pd.read_sql('SELECT * FROM result', con=cnx )
results


# In[56]:


##use crontab to update database every 10 minutes
# open terminal- enter crontab -e - edit the content: put in *10 * * * * python /Users/louluying/yelp/final_yelp_business_match.py - save and quit
##track whether the database update 
import logging

filename ='/Users/louluying/yelp/yelp.log' #create a file storing log information
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler(filename)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)

def do_logging():
    logger.info('Yelp database updated.')

if __name__ == '__main__':
   do_logging()

