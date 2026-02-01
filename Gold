# Databricks notebook source
# MAGIC %md
# MAGIC # 00 Config

# COMMAND ----------

import sys
import time
from time import sleep
import datetime
import splunklib.client as client
#from utils import parse
from datetime import timedelta
import splunklib.results as results
import pandas as pd
from io import BytesIO
import sqlalchemy as sa
import urllib
from dateutil import parser
from functools import reduce
import os
import snowflake.connector as snow
import pyodbc
from openpyxl import load_workbook
from snowflake.connector.pandas_tools import write_pandas
import numpy as np
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.client_credential import ClientCredential
from office365.sharepoint.files.file import File
from pyspark.sql import functions as F
from pyspark.sql.functions import lit
from datetime import datetime
import io
from datetime import date
from urllib.parse import quote
import snowflake.connector 
import base64 
import random 
import json 
import ssl 
from urllib.parse import urlparse, urlunparse, urlencode, quote 
from tabulate import tabulate 
import requests
import string

snflk_refresh_token = dbutils.secrets.get(
    scope="azure_key_vault_backed_databricks_scope",
    key="RSC-PRD-SNF-PBOARD-DI-RefreshToken",
)


snflk_client_secret = dbutils.secrets.get(
    scope="azure_key_vault_backed_databricks_scope",
    key="Snowflake-PI-Client-Secret",
)

snflk_client_id = dbutils.secrets.get(
    scope="azure_key_vault_backed_databricks_scope",
    key="Snowflake-PI-Client-Id",
)

# COMMAND ----------

client_id = snflk_client_id 
client_secret = snflk_client_secret
redirect_uri = 'https://localhost.com' 
authorization_endpoint = 'https://tmobile.west-us-2.privatelink.snowflakecomputing.com/oauth/authorize' 
token_endpoint = 'https://tmobile.west-us-2.privatelink.snowflakecomputing.com/oauth/token-request' 

# This need to be change based on per user step 4 output 
# Copy the refresh_token from the above Curl output #refresh_token='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx' 
refresh_token=snflk_refresh_token

# Generate Access Token 
hdrs = {'Authorization': 'Basic {}'.format(base64.b64encode('{}:{}'.format(client_id, client_secret).encode()).decode()), 
    'Content-type': 'application/x-www-form-urlencoded;charset=utf-8'} 
data = urlencode({ 
        'grant_type': 'refresh_token', 
        'refresh_token': refresh_token, 
        'redirect_uri': redirect_uri 
    }) 

data = data.encode('ascii') 
r = requests.post( 
    token_endpoint, 
    headers=hdrs, 
    data=data) 

access_token = r.json()['access_token'] 
#print('access token: ' + access_token) 
# Connect using OAuth, change User# to your email id and uncomment the account you want to connect 
ctx = snowflake.connector.connect( 
    user="RSC_PRD_SF_PBOARD_DI@T-MOBILE.COM", 
    account='tmobile.west-us-2.privatelink', 
    authenticator='oauth', 
    warehouse='BDM_PPDA_DI_PRD_WH_01', 
    database='from BDM_PPDA_DB', 
    schema = 'PROD_BOARD_T', 
    token=access_token 
)    

cur = ctx.cursor() 
query ="""select MAX(SN_CREATED_TIME_PST) from  BDM_PPDA_DB.PROD_BOARD_T.T_TSENTIMENT_UNIFIED_WHITE_GOLD where MAIN_SOURCE = 'Sprinklr'""" 
cur.execute(query) 
data=pd.DataFrame.from_records(iter(cur), columns=[x[0] for x in cur.description]) 
#print(tabulate(data, headers='keys', tablefmt='psql')) 
Date = data.iloc[0, 0]
print(Date)

# Close connections 
#cur.close() 
#ctx.close() 

# COMMAND ----------

# MAGIC %md
# MAGIC # 01 - Load from Snowflake

# COMMAND ----------

# -------------------------
# Snowflake OAuth (expects workspace secrets/vars: snflk_client_id, snflk_client_secret, snflk_refresh_token)
# -------------------------
client_id = snflk_client_id
client_secret = snflk_client_secret
redirect_uri = 'https://localhost.com'
token_endpoint = 'https://tmobile.west-us-2.privatelink.snowflakecomputing.com/oauth/token-request'
refresh_token = snflk_refresh_token

hdrs = {
    'Authorization': 'Basic {}'.format(
        base64.b64encode(f'{client_id}:{client_secret}'.encode()).decode()
    ),
    'Content-type': 'application/x-www-form-urlencoded;charset=utf-8'
}
data = urlencode({
    'grant_type': 'refresh_token',
    'refresh_token': refresh_token,
    'redirect_uri': redirect_uri
}).encode('ascii')

r = requests.post(token_endpoint, headers=hdrs, data=data)
r.raise_for_status()
access_token = r.json()['access_token']

ctx = snowflake.connector.connect(
    user="RSC_PRD_SF_PBOARD_DI@T-MOBILE.COM",
    account='tmobile.west-us-2.privatelink',
    authenticator='oauth',
    warehouse='BDM_PPDA_DI_PRD_WH_01',
    database='BDM_PPDA_DB',
    schema='PROD_BOARD_T',
    token=access_token
)
cur = ctx.cursor()

# -------------------------
# Incremental fetch: Bronze rows whose ID is NOT present in Silver (anti-join)
# - Compare as STRING to avoid numeric/string mismatches
# - Skips NULL/empty IDs and hash rows (removed due to resyndication policies) from Bronze
# -------------------------
query = f"""SELECT  
        S1.SILVER_ID,
	    B1.ID as BRONZE_ID,
	    B1.ACCOUNT_ID,
	    B1.TOPIC_IDS, 
        'Sprinklr' as MAIN_SOURCE,
        B1.SOURCE, 
        B1.FROM_SN_USER, 
        B1.MESSAGE_TYPE, 
	    B1.MESSAGE_CONTENT, 
        S1.AI_SUMMARY, 
        /* Sentiment mapping */
        CASE
          WHEN LOWER(S1.AI_SENTIMENT_LABEL) = 'positive' THEN 'Positive'
          WHEN LOWER(S1.AI_SENTIMENT_LABEL) = 'neutral'  THEN 'Positive'
          WHEN LOWER(S1.AI_SENTIMENT_LABEL) = 'negative' THEN 'Negative'
          ELSE NULL
        END AS AI_SENTIMENT, 
	    S1.AI_TOPICS, 
        S1.AI_TOPIC_1, 
        S1.AI_TOPIC_2,
	    S1.AI_LLM_TOPICS, 
        S1.AI_LLM_TOPIC_1, 
	    S1.AI_LLM_TOPIC_2, 
        M1.STRATEGIC_PILLAR, 
        M1.CATEGORY, 
        M1.THEME, 
        M1.MAPPING_METHOD, 
        M1.CONFIDENCE_SCORE,
	    B1.PERMALINK,
        S1.AI_ERROR,
	    B1.SN_CREATED_TIME_UTC,
	    B1.SN_CREATED_TIME_PST,
	    B1.LOAD_DATE AS LOAD_DATE_SILVER,
        /* Consolidated_Brand_Executive */
        CASE
          WHEN B1.TOPIC_IDS IS NOT NULL AND B1.TOPIC_IDS <> '' THEN
            CASE
              WHEN B1.TOPIC_IDS LIKE '%T-Mobile%' 
                OR B1.TOPIC_IDS LIKE '%Tmobile%' 
                OR B1.TOPIC_IDS LIKE '%Listening%' 
                OR B1.TOPIC_IDS LIKE '%TMobile%' THEN 'T-Mobile'
              WHEN B1.TOPIC_IDS LIKE '%[CI] AT&T%' THEN 'AT&T'
              WHEN B1.TOPIC_IDS LIKE '%[CI] Verizon%' THEN 'Verizon'
              WHEN B1.TOPIC_IDS LIKE '%Deeane King%' THEN 'Deeanne King'
              WHEN B1.TOPIC_IDS LIKE '%Jeff Simon%' THEN 'Jeff Simon'
              WHEN B1.TOPIC_IDS LIKE '%John Saw%' THEN 'John Saw'
              WHEN B1.TOPIC_IDS LIKE '%Jon Freier%' THEN 'Jon Freier'
              WHEN B1.TOPIC_IDS LIKE '%Mark Nelson%' THEN 'Mark Nelson'
              WHEN B1.TOPIC_IDS LIKE '%Omar Tazi%' THEN 'Omar Tazi'
              WHEN B1.TOPIC_IDS LIKE '%Srini Gopalan%' THEN 'Srini Gopalan'
              WHEN B1.TOPIC_IDS LIKE '%Peter Osvaldik%' THEN 'Peter Osvaldik'
              WHEN B1.TOPIC_IDS LIKE '%Sievert%' THEN 'Mike Sievert'
              WHEN B1.TOPIC_IDS LIKE '%Mike Katz%' THEN 'Mike Katz'
              WHEN B1.TOPIC_IDS LIKE '%Andre Almeida%' THEN 'André Almeida'
              ELSE B1.TOPIC_IDS
            END
              ELSE
            CASE
              WHEN B1.ACCOUNT_ID LIKE '%T-Mobile%' 
                OR B1.ACCOUNT_ID LIKE '%Tmobile%' 
                OR B1.ACCOUNT_ID LIKE '%Listening%' 
                OR B1.ACCOUNT_ID LIKE '%TMobile%' THEN 'T-Mobile'
              WHEN B1.ACCOUNT_ID LIKE '%Deeane King%' THEN 'Deeanne King'
              WHEN B1.ACCOUNT_ID LIKE '%Jeff Simon%' THEN 'Jeff Simon'
              WHEN B1.ACCOUNT_ID LIKE '%John Saw%' THEN 'John Saw'
              WHEN B1.ACCOUNT_ID LIKE '%Jon Freier%' THEN 'Jon Freier'
              WHEN B1.ACCOUNT_ID LIKE '%Mark Nelson%' THEN 'Mark Nelson'
              WHEN B1.ACCOUNT_ID LIKE '%Omar Tazi%' THEN 'Omar Tazi'
              WHEN B1.ACCOUNT_ID LIKE '%Srini Gopalan%' THEN 'Srini Gopalan'
              WHEN B1.ACCOUNT_ID LIKE '%Peter Osvaldik%' THEN 'Peter Osvaldik'
              WHEN B1.ACCOUNT_ID LIKE '%Sievert%' THEN 'Mike Sievert'
              WHEN B1.ACCOUNT_ID LIKE '%Mike Katz%' THEN 'Mike Katz'
              WHEN B1.ACCOUNT_ID LIKE '%Andre Almeida%' THEN 'André Almeida'
              ELSE B1.ACCOUNT_ID
          END
        END AS Consolidated_Brand_Executive
    FROM BDM_PPDA_DB.PROD_BOARD_T.TSENTIMENT_SPRINKLR_BRONZE B1
        LEFT JOIN BDM_PPDA_DB.PROD_BOARD_T.TSENTIMENT_SPRINKLR_SILVER S1 
               ON B1.ID=S1.BRONZE_ID
        LEFT JOIN BDM_PPDA_DB.PROD_BOARD_T.MAP_LLM_TOPIC_TO_CANONICAL M1
               ON LOWER(S1.AI_LLM_TOPIC_1) = M1.AI_LLM_TOPIC_1_NORM
    where B1.SN_CREATED_TIME_PST >= '{Date}'           
;
"""

cur.execute(query)
df_pd = cur.fetch_pandas_all()


print(df_pd)


# COMMAND ----------

from pyspark.sql.functions import current_timestamp, date_format

df = spark.createDataFrame(df_pd).withColumn('LOAD_DATE', date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss'))

display(df)

# COMMAND ----------

from pyspark.sql.functions import col, date_format

df = (
    df.withColumn(
        "SN_CREATED_TIME_UTC",
        date_format(col("SN_CREATED_TIME_UTC"), "yyyy-MM-dd HH:mm:ss")
    )
    .withColumn(
        "SN_CREATED_TIME_PST",
        date_format(col("SN_CREATED_TIME_PST"), "yyyy-MM-dd HH:mm:ss")
    )
    .withColumn(
        "LOAD_DATE_SILVER",
        date_format(col("LOAD_DATE_SILVER"), "yyyy-MM-dd HH:mm:ss")
    )
)


display(df)

# COMMAND ----------

client_id = snflk_client_id 
client_secret = snflk_client_secret
redirect_uri = 'https://localhost.com' 
authorization_endpoint = 'https://tmobile.west-us-2.privatelink.snowflakecomputing.com/oauth/authorize' 
token_endpoint = 'https://tmobile.west-us-2.privatelink.snowflakecomputing.com/oauth/token-request' 
refresh_token=snflk_refresh_token

# Generate Access Token 

hdrs = {'Authorization': 'Basic {}'.format(base64.b64encode('{}:{}'.format(client_id, client_secret).encode()).decode()), 

    'Content-type': 'application/x-www-form-urlencoded;charset=utf-8'} 

 

data = urlencode({ 

        'grant_type': 'refresh_token', 

        'refresh_token': refresh_token, 

        'redirect_uri': redirect_uri 

    }) 

data = data.encode('ascii') 

 

r = requests.post( 

    token_endpoint, 

    headers=hdrs, 

    data=data) 

 

 

access_token = r.json()['access_token'] 

#print('access token: ' + access_token) 

 

snflk_conn = snowflake.connector.connect( 

    user="RSC_PRD_SF_PBOARD_DI@T-MOBILE.COM", 

    account='tmobile.west-us-2.privatelink', 

    authenticator='oauth', 

    warehouse='BDM_PPDA_DI_PRD_WH_01', 

    database='BDM_PPDA_DB', 

    schema = 'PROD_BOARD_T', 

    token=access_token 

)    

cur = snflk_conn.cursor() 

# COMMAND ----------

# Convert Spark DataFrame to Pandas DataFrame
pandas_df = df.toPandas()

# Convert column names to uppercase
pandas_df.columns = [col.upper() for col in pandas_df.columns]



write_pandas(snflk_conn, pandas_df, "T_TSENTIMENT_UNIFIED_WHITE_GOLD", auto_create_table=False, overwrite=False)

