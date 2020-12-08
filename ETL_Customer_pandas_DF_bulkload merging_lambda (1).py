#!/usr/bin/env python
# coding: utf-8

# # EXTRACTION

# In[6]:


import sqlalchemy #Borrow API/Package/Library for accessing databases in generel 
import mysql #Borrow Driver for accessing mysql DB in specific 

#My local database URL
sanjaydatabaseurl = 'mysql://root:4646@localhost/dvdrental?charset=utf8mb4&binary_prefix=true'
print('estabilishing database connection.inprogress...')


# In[7]:


from sqlalchemy import create_engine #Spend Decide API/Package/Library for accessing databases in generel
engine = create_engine(sanjaydatabaseurl) #Spend Plan API/Package/Library for accessing databases in generel
connection = engine.connect() #Spend the API/money API/Package/Library for accessing databases in generel
connection = connection.execution_options(isolation_level="READ COMMITTED") # just READ

from sqlalchemy.sql import text
MyQuery = text("select * from customer") #Enjoy the be1`nfits of API API/Package/Library for accessing databases in generel
sanjayrows = connection.execute(MyQuery).fetchall() 
print(sanjayrows)


# In[8]:


#type(sanjayrow)


# In[9]:


#for i in range(len(sanjayrow)):
    #print(sanjayrow[i][4])  
    
#import pandas as pd
#df=sanjayrows
#df
import pandas as pd
df = pd.DataFrame(sanjayrows)

#df[[4,5]] brings data for 4th and 5th columns

#df.iloc[:10] #brings data for rows
#df.iloc[:,:5] #brings both rows and columns
df[df[4]=='elizabeth.brown@sakilacustomer.org']# brings data based on a condition

#df.tail(10)
#df.head(5)
#df.columns


# In[10]:



from sqlalchemy.sql import text
MyQuery1 = text("select * from address") #Enjoy the be1`nfits of API API/Package/Library for accessing databases in generel
sanjayrows1 = connection.execute(MyQuery1).fetchall() 
print(sanjayrows1)


# In[11]:


#Create Dummy Data

#df_a = pd.DataFrame({
        #'subject_id': ['1', '2', '3', '4', '5'],
        #'first_name': ['Alex', 'Amy', 'Allen', 'Alice', 'Ayoung'],
        #'last_name': ['Anderson', 'Ackerman', 'Ali', 'Aoni', 'Atiches']})
        
df_a = pd.DataFrame(sanjayrows)        
               
#df_b = pd.DataFrame({
        #'subject_id': ['4', '5', '6', '7', '8'],
        #'first_name': ['Billy', 'Brian', 'Bran', 'Bryce', 'Betty'],
        #'last_name': ['Bonder', 'Black', 'Balwner', 'Brice', 'Btisan']})
        
df_b = pd.DataFrame(sanjayrows1)        
               
#df_c = pd.DataFrame({
        #'subject_id': ['1', '2', '3', '4', '5', '7', '8', '9', '10', '11'],
        #'test_id': ['51', '15', '15', '61', '16', '14', '15', '1', '61', '16']})
        
#df_c = pd.DataFrame(sanjayrows2)          


# In[12]:




df_a.columns = ['customer_id', 'store_id', 'first_name', 'last_name', 'email', 'address_id', 'active bool', 'create_date', 'lat_update', 'active', 'phone_number']


# In[13]:


df_b.columns = ['address_id', 'address', 'address2', 'district', 'city_id', 'postal_code', 'phone', 'last_update']
df_b.columns
print(df_b)


# In[14]:



pd.merge(df_a, df_b, on='address_id', how='inner')


# In[15]:


df_b.apply(lambda x: x)


# In[16]:


#df_b.apply(lambda x: x[0])
df_b.apply(lambda x: x[0], axis=1)


# In[17]:


#for i in range(len(df_b)):

     #print(df_b[i]) #It will not work
  


# In[18]:


pd.merge(df_a, df_b, on='address_id', how='right')


# In[19]:


pd.merge(df_a, df_b, on='address_id', how='left')


# In[ ]:





# # LOAD

# In[5]:


import sqlalchemy #Borrow API/Package/Library for accessing databases in general 
import mysql #Borrow Driver for accessing postgres DB in specific 

#My local database URL
sanjaydatabaseurl = 'mysql://root:4646@localhost/dbsanju?charset=utf8mb4&binary_prefix=true'


# In[7]:


#New Fashioned Code
from sqlalchemy import create_engine 
engine = create_engine(sanjaydatabaseurl)
connection = engine.connect() 
connection = connection.execution_options(isolation_level="AUTOCOMMIT") 

from sqlalchemy import Boolean 
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
metadata = MetaData()
Load2table = Table('load2', metadata,
        Column('customer_id', Integer),
        Column('store_id', Integer),
        Column('first_name', String),
        Column('last_name', String),
        Column('email', String),
        Column('address_id', String),
        Column('activebool', String),
        Column('create_date',String),
        Column('last_update',String),
        Column('active',Integer),
        Column('phone_number',String)    )

#This for loop iterates for 564 times
#Assuming i starts with 0
#elizabeth.brown@sakilacustomer.org	 - This value is manually identified as 3rd row 4th column.That means sanjayrows[3][4] 
#print(sanjayrows[3][4]) - just for Testing

for i in range(len(sanjayrows)):
#if(sanjayrows[i][4] === "elizabeth.brown@sakilacustomer.org"):
if(i==4):
ins = Load2table.insert().values(customer_id=sanjayrows[i][0], store_id=sanjayrows[i][1], first_name=sanjayrows[i][2], last_name=sanjayrows[i][3], email=sanjayrows[i][4], address_id=sanjayrows[i][5], activebool=sanjayrows[i][6],create_date=sanjayrows[i][7],last_update=sanjayrows[i][8],active=sanjayrows[i][9],phone_number=sanjayrows[i][10])

    #This is the last step for committing a row
connection.execute(ins)

else:
print("nomatch")
    






connection.close()


# In[ ]:




