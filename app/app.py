#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests


# In[2]:


import pandas as pd


# In[3]:


import psycopg2


# Предварительно изучаем ресурс для получения курсов валют.
# Получаем ключ доступа и определяем конечную точку (endpoint) для получения требуемой нам информации.

# In[4]:


url = 'http://api.exchangerate.host/timeframe'


# In[5]:


params = {"access_key": "510707589c6d083e9d6de8cf0d16a811", "start_date": "2023-02-01","end_date":"2023-02-28", "source":"BTC", "currencies":"RUB", "format":"1" }


# In[6]:


response = requests.get(url,params=params)


# In[7]:


response


# In[8]:


a=response.json()
a


# Модифицируем полученный словарь для дальнейшего использования при создании БД в постгрис 

# In[9]:


rate_date=list(a['quotes'])
rate_date


# In[10]:


rate=pd.DataFrame(a['quotes'].values()).round(2).squeeze().tolist()
rate


# In[11]:


cur_f= ["BTC"]*len(rate)
cur_s= ["RUB"]*len(rate)


# In[12]:


data=list(zip(rate_date, cur_f, cur_s, rate))
data


# Создаем БД

# In[13]:


conn = psycopg2.connect(dbname="database", user="postgres", password="postgres", port="5432", host="db")
cursor = conn.cursor()
 
conn.autocommit = True
sql = "CREATE DATABASE btsrate"
 
cursor.execute(sql)
print("База данных успешно создана")
 
cursor.close()
conn.close()


# In[14]:


conn = psycopg2.connect(dbname="btsrate", user="postgres", password="postgres", port="5432", host="db")
cursor = conn.cursor()
 
cursor.execute("CREATE TABLE monthrate (id SERIAL PRIMARY KEY, rate_date date, currency_f VARCHAR(50), currency_s VARCHAR(50), rate real)")
conn.commit()

print("Таблица успешно создана")
 
cursor.close()
conn.close()


# In[15]:


conn = psycopg2.connect(dbname="btsrate", user="postgres", password="postgres", port="5432", host="db")
cursor = conn.cursor()
 
cursor.executemany("INSERT INTO monthrate (rate_date, currency_f , currency_s , rate ) VALUES (%s, %s, %s, %s)", data)
 
conn.commit()  
print("Данные добавлены")
 
cursor.close()
conn.close()


# In[ ]:


conn = psycopg2.connect(dbname="btsrate", user="postgres", password="postgres", port="5432", host="db")
cursor = conn.cursor()
 
cursor.execute("SELECT * FROM monthrate")
table1=cursor.fetchall()

print(table1)   
 
cursor.close()
conn.close()


# In[ ]:





# Извлекаем нужные нам данные из таблицы

# In[16]:


conn = psycopg2.connect(dbname="btsrate", user="postgres", password="postgres", port="5432", host="db")
cursor = conn.cursor()
 
cursor.execute("SELECT max(rate) FROM monthrate")
max_rate=cursor.fetchone()

print(max_rate)   
 
cursor.close()
conn.close()


# In[17]:


conn = psycopg2.connect(dbname="btsrate", user="postgres", password="postgres", port="5432", host="db")
cursor = conn.cursor()
 
cursor.execute("SELECT rate_date FROM monthrate where rate = (SELECT max(rate) FROM monthrate)")
max_rate_date=cursor.fetchone()

print(max_rate_date)   
 
cursor.close()
conn.close()


# In[18]:


conn = psycopg2.connect(dbname="btsrate", user="postgres", password="postgres", port="5432", host="db")
cursor = conn.cursor()
 
cursor.execute("SELECT min(rate) FROM monthrate")
min_rate=cursor.fetchone()

print(min_rate)   
 
cursor.close()
conn.close()


# In[19]:


conn = psycopg2.connect(dbname="btsrate", user="postgres", password="postgres", port="5432", host="db")
cursor = conn.cursor()
 
cursor.execute("SELECT rate_date FROM monthrate where rate = (SELECT min(rate) FROM monthrate)")
min_rate_date=cursor.fetchone()

print(min_rate_date)   
 
cursor.close()
conn.close()


# In[20]:


conn = psycopg2.connect(dbname="btsrate", user="postgres", password="postgres", port="5432", host="db")
cursor = conn.cursor()
 
cursor.execute("SELECT avg(rate) FROM monthrate")
avg_rate=cursor.fetchone()

print(avg_rate)   
 
cursor.close()
conn.close()


# In[21]:


conn = psycopg2.connect(dbname="btsrate", user="postgres", password="postgres", port="5432", host="db")
cursor = conn.cursor()
 
cursor.execute("SELECT rate FROM monthrate where rate_date=('2023-02-28')")
last_day_rate=cursor.fetchone()

print(last_day_rate)   
 
cursor.close()
conn.close()


# In[22]:


month="2023-02-01"
cf="BTC"
cs="RUB"


# In[23]:


data_mart=list(zip((month,), (cf,), (cs,), max_rate, max_rate_date,min_rate,min_rate_date, avg_rate, last_day_rate))
data_mart


# Создаем сводную таблицу с показателями

# In[24]:


conn = psycopg2.connect(dbname="btsrate", user="postgres", password="postgres", port="5432", host="db")
cursor = conn.cursor()
 
cursor.execute("CREATE TABLE mart (month date, currency_f VARCHAR(50), currency_s VARCHAR(50), max_rate real, max_rate_date date,min_rate real ,min_rate_date date, avg_rate real, last_day_rate real)")
conn.commit()

print("Таблица успешно создана")
 
cursor.close()
conn.close()


# In[25]:


conn = psycopg2.connect(dbname="btsrate", user="postgres", password="postgres", port="5432", host="db")
cursor = conn.cursor()
 
cursor.executemany("INSERT INTO mart (month , currency_f, currency_s, max_rate, max_rate_date ,min_rate  ,min_rate_date , avg_rate, last_day_rate) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", data_mart)
 
conn.commit()  
print("Данные добавлены")
 
cursor.close()
conn.close()


# In[ ]:


conn = psycopg2.connect(dbname="btsrate", user="postgres", password="postgres", port="5432", host="db")
cursor = conn.cursor()
 
cursor.execute("SELECT * FROM mart")
table2=cursor.fetchall()

print(table2)   
 
cursor.close()
conn.close()

