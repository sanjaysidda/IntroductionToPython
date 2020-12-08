#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sqlalchemy #Borrow API/Package/Library for accessing databases in generel 
import mysql #Borrow Driver for accessing mysql DB in specific 

#My local database URL
sanjaydatabaseurl = 'mysql://root:4646@localhost/dvdrental?charset=utf8mb4&binary_prefix=true'
print('estabilishing database connection.inprogress...')


# In[2]:


from sqlalchemy import create_engine #Spend Decide API/Package/Library for accessing databases in generel
engine = create_engine(sanjaydatabaseurl) #Spend Plan API/Package/Library for accessing databases in generel
connection = engine.connect() #Spend the API/money API/Package/Library for accessing databases in generel
connection = connection.execution_options(isolation_level="READ COMMITTED") # just READ


# In[3]:


from sqlalchemy.sql import text
MyQuery = text("select * from address") #Enjoy the be1`nfits of API API/Package/Library for accessing databases in generel
sanjayrows = connection.execute(MyQuery).fetchall() 
print(sanjayrows)


# In[4]:


import pandas as pd
import numpy as np
data_sanjayrows = pd.DataFrame(sanjayrows)
data_sanjayrows.columns = ['address_id', 'address', 'address2', 'district', 'city_id', 'postal_code', 'phone', 'last_update']
#data_sanjayrows
#accessing row wise
data_sanjayrows.apply(lambda x: x)
#connection.close()


# In[5]:


# access first row
data_sanjayrows.apply(lambda x: x[0])


# In[6]:


# access first column by index
data_sanjayrows.apply(lambda x: x[0], axis=1)


# In[7]:


#access by column name
data_sanjayrows.apply(lambda x: x["address"], axis=1)[:5]


# In[8]:


#import pandas as pd
#data_sanjayrows = pd.DataFrame(sanjayrows)
#data_sanjayrows.apply(lambda x: x)
#data_sanjayrows.columns = ['address_id', 'address', 'address2', 'district', 'city_id', 'postal_code', 'phone', 'last_update']


# In[9]:


# before clipping
data_sanjayrows["city_id"][:5]


# In[11]:


# clip city_id if it is greater than 0 and less than equat to 200
def clip_city_id(city_id):
    if city_id > 0 and city_id <=200 :
        city_id = "Apple"
        return city_id
    
# after clipping
data_sanjayrows["city_id"].apply(lambda x: clip_city_id(x))[:200]


# In[12]:


#Modified_city_id = int(city_id)
def clip_city_id(city_id):
    if city_id > 0 and city_id <=200 :
        city_id = "apple"
    elif city_id > 200 and city_id <=300:
        city_id = "mango"
    elif city_id >400 and city_id <=500:
        city_id = "orange"
    else:
        city_id = "banana"
    return city_id

Fruits_output = data_sanjayrows["city_id"].apply(lambda x: clip_city_id(x)) [:200]
#state_output.rename(columns = {'city_id':'Fruits'}, inplace = True)
#print(Fruits_output)
#print(data_sanjayrows)
#result = pd.merge(data_sanjayrows,pd.DataFrame(Fruits_output), on='city_id', how='right')
#result
#pd.merge(data_sanjayrows, pd.DataFrame(Fruits_output))
#type(pd.DataFrame(Fruits_output))
#type(data_sanjayrows)
result = pd.concat([data_sanjayrows,pd.DataFrame(Fruits_output)], axis=1)
result.columns = ['address_id', 'address', 'address2', 'district', 'city_id', 'postal_code', 'phone', 'last_update', 'Fruits']
pd.DataFrame(result)


# In[ ]:


def clip_city_id(city_id):
    if city_id > 0 and city_id <=200 :
        city_id = "Apple"
    elif city_id > 200 and city_id <=300:
        city_id = "Mango"
    elif city_id >400 and city_id <=500:
        city_id = "Orange"
    else:
        city_id = "Banana"
    return city_id

#data_sanjayrows["city_id"].apply(lambda x: clip_city_id(x)) [:200]
data_sanjayrows

