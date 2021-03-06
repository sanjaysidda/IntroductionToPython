#!/usr/bin/env python
# coding: utf-8

# # EXTRACTION

# In[8]:


# My local database URL
sanjaydatabaseurl = 'mysql://root:4646@localhost/dvdrental?charset=utf8mb4&binary_prefix=true'
print('estabilishing database connection.inprogress...')

# In[9]:


from sqlalchemy import create_engine  # Spend Decide API/Package/Library for accessing databases in generel

engine = create_engine(sanjaydatabaseurl)  # Spend Plan API/Package/Library for accessing databases in generel
connection = engine.connect  # Spend the API/money API/Package/Library for accessing databases in generel
connection = connection.execution_options(isolation_level="READ COMMITTED")  # just READ)

from sqlalchemy.sql import text

MyQuery = text(
    "select * from customer")  # Enjoy the be1`nfits of API API/Package/Library for accessing databases in generel
sanjayrows = connection.execute(MyQuery).fetchall()
print(sanjayrows)

# In[10]:


# type(sanjayrow)


# In[27]:


# for i in range(len(sanjayrow)):
# print(sanjayrow[i][4])

# import pandas as pd
# df=sanjayrows
# df
import pandas as pd

df = pd.DataFrame(sanjayrows)
# df[[4,5]] brings data for 4th and 5th columns

# df.iloc[:10] #brings data for rows
# df.iloc[:,:5] #brings both rows and columns
df[df[4] == 'elizabeth.brown@sakilacustomer.org']  # brings data based on a condition

# df.tail(10)
# df.head(5)
# df.columns


# # LOAD

# In[5]:


# My local database URL
sanjaydatabaseurl = 'mysql://root:4646@localhost/dbsanju?charset=utf8mb4&binary_prefix=true'

# In[7]:


# New Fashioned Code
from sqlalchemy import create_engine

engine = create_engine(sanjaydatabaseurl)
connection = engine.connect()
connection = connection.execution_options(isolation_level="AUTOCOMMIT")

from sqlalchemy import Table, Column, Integer, String, MetaData

metadata = MetaData()
Load2table = Table('load2', metadata,
                   Column('customer_id', Integer),
                   Column('store_id', Integer),
                   Column('first_name', String),
                   Column('last_name', String),
                   Column('email', String),
                   Column('address_id', String),
                   Column('activebool', String),
                   Column('create_date', String),
                   Column('last_update', String),
                   Column('active', Integer),
                   Column('phone_number', String))

# This for loop iterates for 564 times
# Assuming i starts with 0
# elizabeth.brown@sakilacustomer.org	 - This value is manually identified as 3rd row 4th column.That means sanjayrows[3][4]
# print(sanjayrows[3][4]) - just for Testing

for i in range(len(sanjayrows)) :
    # if(sanjayrows[i][4] === "elizabeth.brown@sakilacustomer.org"):
    if (i == 4) :
        ins = Load2table.insert().values(customer_id=sanjayrows[i][0], store_id=sanjayrows[i][1],
                                         first_name=sanjayrows[i][2], last_name=sanjayrows[i][3],
                                         email=sanjayrows[i][4], address_id=sanjayrows[i][5],
                                         activebool=sanjayrows[i][6], create_date=sanjayrows[i][7],
                                         last_update=sanjayrows[i][8], active=sanjayrows[i][9],
                                         phone_number=sanjayrows[i][10])
        # This is the last step for committing a row
        connection.execute(ins)
    else :
        print("nomatch")

connection.close()

# In[ ]:
