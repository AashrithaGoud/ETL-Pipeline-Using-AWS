#!/usr/bin/env python
# coding: utf-8

# In[2]:


import boto3
import pandas as pd
import time
import botocore
from io import StringIO
import redshift_connector


# In[2]:


AWS_ACCESS_KEY_ID = "AKIAUOMJHP2S7HPIE5EL"
AWS_SECRET_ACCESS_KEY = "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"
AWS_REGION = "us-east-1"
SCHEMA_NAME = "myntra_database"
S3_STAGING_DIR = "s3://myntraoutputdata/Unsaved/"
S3_BUCKET_NAME = "myntraoutputdata"
S3_OUTPUT_DIR = "Unsaved/2023/10/06/821bd513-7811-45fb-94f0-4a0f33980387.csv"


# In[3]:


athena_client = boto3.client(
    "athena",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)


# In[4]:


myntraoutput = athena_client.start_query_execution(
    QueryString='SELECT * FROM myntra_database',
    QueryExecutionContext={
        'Database': SCHEMA_NAME
    },
    ResultConfiguration={
        'OutputLocation': S3_STAGING_DIR
    }
)


# In[5]:


s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)


# In[6]:


s3_client.download_file(S3_BUCKET_NAME,S3_OUTPUT_DIR, myntraoutput['QueryExecutionId'] + ".csv")


# In[7]:


df = pd.read_csv(myntraoutput['QueryExecutionId']+".csv")


# In[8]:


df.head(10)


# In[9]:


df = df.drop('partition_0', axis=1)


# In[10]:


df.head()


# In[11]:


df['ratings'] = df['ratings'].fillna(0)


# In[12]:


df.head()


# In[13]:


Fact_Product=df[['product_id','url','brandname','discountprice (in rs)','originalprice (in rs)','discountoffer']]


# In[14]:


Fact_Product.head()


# In[15]:


Dim_Category=df[['product_id','url','brandname','category','individual_category','category_by_gender','description']]


# In[16]:


Dim_Category.head()


# In[17]:


Dim_Product_Reviews=df[['product_id','sizeoption','ratings','reviews']]


# In[18]:


Dim_Product_Reviews.head()


# In[19]:


Dim_Product_Sales=df[['product_id','discountprice (in rs)','originalprice (in rs)','discountoffer']]


# In[20]:


Dim_Product_Sales.head()


# In[21]:


bucket='myntrainputdata'


# In[22]:


csv_buffer_Fact_Product=StringIO()
Fact_Product.to_csv(csv_buffer_Fact_Product)
s3_resource = boto3.resource('s3')
s3_resource.Object(bucket,'output/Fact_Product.csv').put(Body=csv_buffer_Fact_Product.getvalue())


# In[23]:


csv_buffer_Dim_Category=StringIO()
Dim_Category.to_csv(csv_buffer_Dim_Category)
s3_resource = boto3.resource('s3')
s3_resource.Object(bucket,'output/Dim_Category.csv').put(Body=csv_buffer_Dim_Category.getvalue())


# In[24]:


csv_buffer_Dim_Product_Reviews=StringIO()
Dim_Product_Reviews.to_csv(csv_buffer_Dim_Product_Reviews)
s3_resource = boto3.resource('s3')
s3_resource.Object(bucket,'output/Dim_Product_Reviews.csv').put(Body=csv_buffer_Dim_Product_Reviews.getvalue())


# In[25]:


csv_buffer_Dim_Product_Sales=StringIO()
Dim_Product_Sales.to_csv(csv_buffer_Dim_Product_Sales)
s3_resource = boto3.resource('s3')
s3_resource.Object(bucket,'output/Dim_Product_Sales.csv').put(Body=csv_buffer_Dim_Product_Sales.getvalue())


# In[26]:


Fact_Product_Schema = pd.io.sql.get_schema(Fact_Product,'Fact_Product')


# In[27]:


print(Fact_Product_Schema)


# In[28]:


Dim_Category_Schema = pd.io.sql.get_schema(Dim_Category,'Dim_Category')
Dim_Product_Reviews_Schema = pd.io.sql.get_schema(Dim_Product_Reviews,'Dim_Product_Reviews')
Dim_Product_Sales_Schema = pd.io.sql.get_schema(Dim_Product_Sales,'Dim_Product_Sales')


# In[29]:


print(Dim_Category_Schema)


# In[ ]:


conn = redshift_connector.connect(
    host='enter the host',
    database='enter',
    user='enter',
    password='enter',
    port=5439
 )

conn.autocommit = True

cursor = redshift_connector.Cursor = conn.cursor()
cursor.execute(csv_buffer_Fact_Product)
cursor.execute(csv_buffer_Dim_Category)
cursor.execute(csv_buffer_Dim_Product_Reviews)
cursor.execute(csv_buffer_Dim_Product_Sales)


