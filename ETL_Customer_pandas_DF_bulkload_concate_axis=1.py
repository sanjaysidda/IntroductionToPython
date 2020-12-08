#!/usr/bin/env python
# coding: utf-8

# # EXTRACTION

# In[106]:


import sqlalchemy #Borrow API/Package/Library for accessing databases in generel 
import mysql #Borrow Driver for accessing mysql DB in specific 

#My local database URL
sanjaydatabaseurl = 'mysql://root:4646@localhost/dvdrental?charset=utf8mb4&binary_prefix=true'
print('estabilishing database connection.inprogress...')


# In[107]:


from sqlalchemy import create_engine #Spend Decide API/Package/Library for accessing databases in generel
engine = create_engine(sanjaydatabaseurl) #Spend Plan API/Package/Library for accessing databases in generel
connection = engine.connect() #Spend the API/money API/Package/Library for accessing databases in generel
connection = connection.execution_options(isolation_level="READ COMMITTED") # just READ

from sqlalchemy.sql import text
MyQuery = text("select * from customer") #Enjoy the be1`nfits of API API/Package/Library for accessing databases in generel
sanjayrows = connection.execute(MyQuery).fetchall() 
print(sanjayrows)


# In[122]:


from sqlalchemy.sql import text
MyQuery2 = text("select * from customer1") #Enjoy the be1`nfits of API API/Package/Library for accessing databases in generel
sanjayrows2 = connection.execute(MyQuery2).fetchall() 
print(sanjayrows2)


# In[123]:



from sqlalchemy.sql import text
MyQuery3 = text("select * from customer2") #Enjoy the be1`nfits of API API/Package/Library for accessing databases in generel
sanjayrows3 = connection.execute(MyQuery3).fetchall() 
print(sanjayrows3)


# In[124]:


#type(sanjayrow)


# In[125]:


#for i in range(len(sanjayrow)):
    #print(sanjayrow[i][4])  
    
#import pandas as pd

#df=sanjayrows
#df
#casting sanjayrows table data to pandas data frame
import pandas as pd
import numpy as np

data_df = pd.DataFrame(sanjayrows)
data_df = data_df.dropna(how="any")
data_df.head()

#df.head()
#df[[4,5]] brings data for 4th and 5th columns

#df.iloc[:10] #brings data for rows
#df.iloc[:,:5] #brings both rows and columns
#df[df[4]=='elizabeth.brown@sakilacustomer.org'] # brings data based on a condition

#df.tail(10)
#df.head(5)
#df.columns


# In[126]:


data_df.sort_values(by=9)
data_df[:5]


# In[127]:


data_df.sort_values(by=5,ascending=False, inplace=True)
data_df[:5]


# In[128]:


data_df.sort_index(inplace=True)
data_df[:5]


# In[129]:


#Create Dummy Data
import pandas as pd

#df1 = pd.DataFrame({'A': ['A0', 'A1', 'A2', 'A3'],
                   #'B':['B0', 'B1', 'B2', 'B3'],
                   #'C':['C0', 'C1', 'C2', 'C3'],
                   #'D':['D0', 'D1', 'D2', 'D3']},
                  #index=[0,1,2,3])
                
df1 = pd.DataFrame(sanjayrows)
#df1.set_index([0,1,2,3,4,5,6,7,8,9,10])

#df2 = pd.DataFrame({'A': ['A4', 'A5', 'A6', 'A7'],
                   #'B':['B4', 'B5', 'B6', 'B6'],
                   #'C':['C4', 'C5', 'C6', 'C6'],
                   #'D':['D4', 'D5', 'D6', 'D6']},
                  #index=[4,5,6,7])

df2 = pd.DataFrame(sanjayrows2)
#df2.set_index([0,1,2,3,4,5,6,7,8,9,10])



#df3 = pd.DataFrame({'A': ['A8', 'A9', 'A10', 'A11'],
                   #'B':['B8', 'B9', 'B10', 'B11'],
                   #'C':['C8', 'C9', 'C10', 'C11'],
                   #'D':['D8', 'D9', 'D10', 'D11']},
                  #index=[8,9,10,11])
                

df3 = pd.DataFrame(sanjayrows3)  
#df3.set_index([0,1,2,3,4,5,6,7,8,9,10])


# In[130]:


#Combine Data Frames

pd.set_option('max_columns', None)
pd.set_option("max_rows", None)

result = pd.concat([df1,df2,df3])
result


# In[131]:


#Combine data frames
result = pd.concat([df1,df2,df3],keys = ['x', 'y', 'z'])
result


# In[132]:


#Get second data frame
#result.loc['y']

result.loc['z']


# In[133]:


#df1 = pd.DataFrame({'A': ['A0', 'A1', 'A2', 'A3'],
                    #'B': ['B0', 'B1', 'B2', 'B3'],
                    #'C': ['C0', 'C1', 'C2', 'C3'],
                    #'D': ['D0', 'D1', 'D2', 'D3']}, 
                 # index=[0,1,2,3])


# In[134]:




#df4 = pd.DataFrame({ 'B':['B2', 'B3', 'B6', 'B7'],
                     #'D':['D2', 'D3', 'D6', 'D7'],
                     #'F':['F2', 'F3', 'F6', 'F7']},
                  #index=[2,3,6,7])

result = pd.concat([df2,df3], axis=1, sort=False)
result               


# In[141]:


print(df1)
#print(df3)


# In[142]:


print(df3)


# In[145]:


result = pd.concat([df1,df3], axis=1, join='inner') # Blind join with indexes
result


# In[ ]:





# # LOAD

# In[26]:


import sqlalchemy #Borrow API/Package/Library for accessing databases in general 
import mysql #Borrow Driver for accessing postgres DB in specific 

#My local database URL
sanjaydatabaseurl = 'mysql://root:4646@localhost/dbsanju?charset=utf8mb4&binary_prefix=true'


# In[27]:


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




