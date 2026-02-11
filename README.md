# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest social data from Sprinklr via API call
# MAGIC
# MAGIC October 2025
# MAGIC
# MAGIC Details about authorizing Sprinklr account and generating auth token elsewhere
# MAGIC
# MAGIC Focus of this notebook is to successfully generate API payloads and load them into Snowflake db

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

# COMMAND ----------


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

# DBTITLE 1,Cell 4
# Databricks cell: fetch Sprinklr (SprinkSights / Listening) report with pagination, return Spark DF
# Assumes you already have a Sprinklr auth token (or can fetch it in a separate cell).
# Set these in Databricks secrets or env:
#   - SPRINKLR_BASE_URL  (e.g., https://<your-tenant>.sprinklr.com)
#   - SPRINKLR_BEARER_TOKEN  (or use your existing OAuth flow)

#import os, json, time
#import requests
#import pandas as pd
#from pyspark.sql import functions as F

import time, json
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# -----------------------
# 1) Config / auth
# -----------------------
BASE_URL = "https://api2.sprinklr.com/prod2/api/v2/reports/query"
TOKEN = os.getenv("SPRINKLR_BEARER_TOKEN")

# Common Sprinklr analytics endpoint (adjust if your tenant uses a different path)
# If your prior notebook already has the correct endpoint, swap it in here.
ENDPOINT = "https://api2.sprinklr.com/prod2/api/v2/reports/query"

HEADERS = {
    'Authorization': 'Bearer Lu62hTQMXpcUGqTl5pOR4Zs57EexPiSqf2WzmXjzMbliMjEzZTM3OS04ZDE1LTM3YzMtYTAwOC1mOGQ2MzE1YWFkNjU=', # USE ACCESS TOKEN (NOT REFRESH TOKEN) from Auth Flow
    'cache-control': 'no-cache',
    'key': '4h7ruvbfxhcsaa5w9z9rches',
    "Content-Type": "application/json",
}

# -----------------------
# 2) Your payload (as-is)
# -----------------------
payload = {
    "report": "SPRINKSIGHTS",
    "reportingEngine": "LISTENING",
    "timeField": None,
    "startTime": 1769932800000,
    "endTime": 1770105599999,
    "timeZone": "America/Los_Angeles",
    "page": 0,
    "pageSize": 20,
    "filters": [
        {
            "dimensionName": "TOPIC_IDS",
            "filterType": "IN",
            "values": [
                "6629305d0e208512f2efc310",
                "66216770a83a8771b7d34f9e",
                "662131f2a83a8771b7769e39",
                "687a9556b903ed6d8880c935"
            ],
            "details": {
                "uniqueId": "D_TOPIC_IDS",
                "contentType": "DB_FILTER",
                "dF": True,
                "DB_FILTER_REPORT_NAME": "SPRINKSIGHTS",
                "OLD_DIM_NAME": "TOPIC",
                "HAS_ALERT": False,
                "nameQueryLookupSupported": "True",
                "DRILLDOWN": False,
                "displayNameForWarning": "Topic",
                "EXIST_FILTER": False,
                "HAS_TOOLTIP_DATA": True
            }
        },
        {
            "dimensionName": "LISTENING_MEDIA_TYPE",
            "filterType": "IN",
            "values": [
                "INSTAGRAM",
                "FACEBOOK",
                "SNAPCHAT",
                "TWITTER",
                "BLUESKY",
                "REDDIT",
                "TIKTOK",
                "YOUTUBE",
                "NEWS",
                "FORUMS"
            ],
            "details": {
                "uniqueId": "D_LISTENING_MEDIA_TYPE",
                "contentType": "DB_FILTER",
                "DB_FILTER_REPORT_NAME": "SPRINKSIGHTS",
                "OLD_DIM_NAME": "LISTENING_MEDIA_TYPE",
                "HAS_ALERT": False,
                "nameQueryLookupSupported": "True",
                "supportedMediaCSV": "ALL_SOURCES",
                "displayNameForWarning": "Source",
                "HAS_TOOLTIP_DATA": True
            }
        },
        {
            "dimensionName": "LST_SUPP_LNG",
            "filterType": "IN",
            "values": [
                "en",
                "es"
            ],
            "details": {
                "uniqueId": "D_LST_SUPP_LNG",
                "contentType": "DB_FILTER",
                "DB_FILTER_REPORT_NAME": "SPRINKSIGHTS",
                "OLD_DIM_NAME": "LANGUAGE",
                "HAS_ALERT": False,
                "nameQueryLookupSupported": "True",
                "supportedMediaCSV": "ALL_SOURCES",
                "displayNameForWarning": "Language",
                "HAS_TOOLTIP_DATA": True
            }
        }
    ],
    "groupBys": [
        {
            "heading": "ES_MESSAGE_ID_0",
            "dimensionName": "ES_MESSAGE_ID",
            "groupType": "FIELD",
            "details": {},
            "namedFilters": None
        },
        {
            "heading": "MESSAGE_CONTENT_1",
            "dimensionName": "MESSAGE_CONTENT",
            "groupType": "FIELD",
            "details": {},
            "namedFilters": None
        },
        {
            "heading": "LISTENING_MEDIA_TYPE_2",
            "dimensionName": "LISTENING_MEDIA_TYPE",
            "groupType": "FIELD",
            "details": {},
            "namedFilters": None
        },
        {
            "heading": "SN_CREATED_TIME_3",
            "dimensionName": "SN_CREATED_TIME",
            "groupType": "DATE_HISTOGRAM",
            "details": {
                "isDateTypeDimension": True,
                "interval": "1d"
            },
            "namedFilters": None
        },
        {
            "heading": "TOPIC_IDS_4",
            "dimensionName": "TOPIC_IDS",
            "groupType": "FIELD",
            "details": {},
            "namedFilters": None
        },
        {
            "heading": "ACCOUNT_ID_5",
            "dimensionName": "ACCOUNT_ID",
            "groupType": "FIELD",
            "details": {},
            "namedFilters": None
        },
        {
            "heading": "FROM_SN_USER_6",
            "dimensionName": "FROM_SN_USER",
            "groupType": "FIELD",
            "details": {},
            "namedFilters": None
        },
        {
            "heading": "SN_MESSAGE_TYPE_7",
            "dimensionName": "SN_MESSAGE_TYPE",
            "groupType": "FIELD",
            "details": {},
            "namedFilters": None
        },
        {
            "heading": "SEM_SENTIMENT_8",
            "dimensionName": "SEM_SENTIMENT",
            "groupType": "FIELD",
            "details": {},
            "namedFilters": None
        },
        {
            "heading": "AGE_CATEGORY_9",
            "dimensionName": "AGE_CATEGORY",
            "groupType": "FIELD",
            "details": {},
            "namedFilters": None
        },
        {
            "heading": "CITY_10",
            "dimensionName": "CITY",
            "groupType": "FIELD",
            "details": {},
            "namedFilters": None
        },
        {
            "heading": "STATE_11",
            "dimensionName": "STATE",
            "groupType": "FIELD",
            "details": {},
            "namedFilters": None
        },
        {
            "heading": "GENDER_12",
            "dimensionName": "GENDER",
            "groupType": "FIELD",
            "details": {},
            "namedFilters": None
        },
        {
            "heading": "HASHTAGS_13",
            "dimensionName": "HASHTAGS",
            "groupType": "FIELD",
            "details": {},
            "namedFilters": None
        },
        {
            "heading": "MSG_EMOTION_14",
            "dimensionName": "MSG_EMOTION",
            "groupType": "FIELD",
            "details": {},
            "namedFilters": None
        },
        {
            "heading": "MSG_EMOTION_CAT_15",
            "dimensionName": "MSG_EMOTION_CAT",
            "groupType": "FIELD",
            "details": {},
            "namedFilters": None
        },
        {
            "heading": "SPECIFIC_TOPIC_GROUP_60da30ccc3ab97721ce4ced3_16",
            "dimensionName": "SPECIFIC_TOPIC_GROUP_60da30ccc3ab97721ce4ced3",
            "groupType": "FIELD",
            "details": {},
            "namedFilters": None
        }
    ],
    "projections": [
        {
            "heading": "M_SPRINKSIGHTS_MENTIONS_COUNT_0",
            "measurementName": "MENTIONS_COUNT",
            "aggregateFunction": "SUM",
            "details": {}
        },
        {
            "heading": "M_SPRINKSIGHTS_REACH_COUNT_1",
            "measurementName": "REACH_COUNT",
            "aggregateFunction": "SUM",
            "details": {}
        },
        {
            "heading": "M_SPRINKSIGHTS_EARNED_ENGAGEMENT_2",
            "measurementName": "EARNED_ENGAGEMENT",
            "aggregateFunction": "SUM",
            "details": {}
        },
        {
            "heading": "M_SPRINKSIGHTS_COMMENTS_COUNT_3",
            "measurementName": "COMMENTS_COUNT",
            "aggregateFunction": "SUM",
            "details": {}
        },
        {
            "heading": "M_SPRINKSIGHTS_LIKES_COUNT_4",
            "measurementName": "LIKES_COUNT",
            "aggregateFunction": "SUM",
            "details": {}
        },
        {
            "heading": "M_SPRINKSIGHTS_SHARES_COUNT_5",
            "measurementName": "SHARES_COUNT",
            "aggregateFunction": "SUM",
            "details": {}
        },
        {
            "heading": "M_SPRINKSIGHTS_TWITTER_RETWEETS_6",
            "measurementName": "TWITTER_RETWEETS",
            "aggregateFunction": "SUM",
            "details": {}
        },
        {
            "heading": "M_SPRINKSIGHTS_MENTIONS_EX_RETWEETS_7",
            "measurementName": "MENTIONS_EX_RETWEETS",
            "aggregateFunction": "SUM",
            "details": {}
        },
        {
            "heading": "M_SPRINKSIGHTS_SAD_COUNT_8",
            "measurementName": "SAD_COUNT",
            "aggregateFunction": "SUM",
            "details": {}
        },
        {
            "heading": "M_SPRINKSIGHTS_ANGER_COUNT_9",
            "measurementName": "ANGER_COUNT",
            "aggregateFunction": "SUM",
            "details": {}
        },
        {
            "heading": "M_SPRINKSIGHTS_LOVE_COUNT_10",
            "measurementName": "LOVE_COUNT",
            "aggregateFunction": "SUM",
            "details": {}
        },
        {
            "heading": "M_SPRINKSIGHTS_FOLLOWERS_COUNT_MEASUREMENT_11",
            "measurementName": "FOLLOWERS_COUNT_MEASUREMENT",
            "aggregateFunction": "MAX",
            "details": {}
        },
        {
            "heading": "M_SPRINKSIGHTS_INFLUENCER_SCORE_12",
            "measurementName": "INFLUENCER_SCORE",
            "aggregateFunction": "MAX",
            "details": {}
        }
    ],
    "projectionDecorations": [],
    "projectionFilters": None,
    "sorts": None,
    "streamRequestInfo": None,
    "additional": {
        "Timezone": "America/Los_Angeles",
        "exportInfo": "False",
        "MARGIN": "False",
        "translateResponse": "False",
        "fetchUnhealthyAccounts": "False",
        "dashboardId": "697a912c1012e93a37a937db",
        "engine": "LISTENING",
        "widgetId": "697a91501012e93a37a95305",
        "showTotal": "False",
        "chartType": "POST_CARD",
        "TABULAR": "True"
    },
    "skipResolve": False,
    "jsonResponse": True
}


def make_session():
    s = requests.Session()
    retry = Retry(
        total=8,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["POST"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    s.mount("https://", adapter)
    return s

SESSION = make_session()

def _sprinklr_post(json_payload: dict, timeout_s: int = 120) -> dict:
    resp = SESSION.post(ENDPOINT, headers=HEADERS, json=json_payload, timeout=timeout_s)
    if not resp.ok:
        raise RuntimeError(
            f"Sprinklr request failed: HTTP {resp.status_code}\n"
            f"URL: {ENDPOINT}\n"
            f"Response: {resp.text[:4000]}"
        )
    return resp.json()

def _extract_rows(obj: dict) -> list[dict]:
    """
    Sprinklr responses vary by widget/report. This tries the common patterns.
    - If your tenant returns a different shape, add a branch here.
    """
    if obj is None:
        return []
    # common candidates
    candidates = [
        obj.get("data"),
        obj.get("rows"),
        obj.get("result"),
        (obj.get("response") or {}).get("data"),
        (obj.get("response") or {}).get("rows"),
        (obj.get("response") or {}).get("result"),
        (obj.get("content") or {}).get("data"),
        (obj.get("content") or {}).get("rows"),
    ]
    for c in candidates:
        if isinstance(c, list):
            return c
        # sometimes wrapped like {"data": {"rows":[...]}}
        if isinstance(c, dict):
            for k in ("rows", "data", "result"):
                if isinstance(c.get(k), list):
                    return c.get(k)
    return []

def fetch_all_pages(
    base_payload: dict,
    page_size: int = 500,          # <= 1000 recommended (timeouts above that) :contentReference[oaicite:4]{index=4}
    max_pages: int = 500,
    max_rows: int = 50_000,        # safety cap; adjust if your endpoint supports more
    sleep_s: float = 0.0
) -> list[dict]:
    all_rows = []
    page = int(base_payload.get("page", 0))

    # Detect pagination loops (same page repeated)
    seen_signatures = set()

    t0 = time.perf_counter()

    for i in range(max_pages):
        p = dict(base_payload)
        p["page"] = page
        p["pageSize"] = page_size

        out = _sprinklr_post(p)
        rows = _extract_rows(out)

        if not rows:
            print(f"Stopping: empty rows at page={page}")
            break

        # signature: first row stable identifier if present, else a hash of first row
        first = rows[0]
        sig = (
            first.get("ES_MESSAGE_ID")
            or first.get("id")
            or hash(json.dumps(first, sort_keys=True))
        )
        if sig in seen_signatures:
            print(f"Stopping: detected repeating page at page={page} (loop protection)")
            break
        seen_signatures.add(sig)

        all_rows.extend(rows)

        elapsed = time.perf_counter() - t0
        print(f"page={page} rows={len(rows)} total={len(all_rows)} elapsed={elapsed:,.1f}s")

        # stop if last page OR safety caps hit
        if len(rows) < page_size:
            print(f"Stopping: last page (rows {len(rows)} < pageSize {page_size})")
            break
        if len(all_rows) >= max_rows:
            print(f"Stopping: reached max_rows={max_rows}")
            break

        page += 1
        if sleep_s:
            time.sleep(sleep_s)

    return all_rows

rows = fetch_all_pages(payload, page_size=500, max_pages=500, max_rows=50_000)
print(f"Fetched rows: {len(rows)}")



# COMMAND ----------

# DBTITLE 1,Cell 5
def stream_pages_as_dataframes(api_url, headers, payload_base, max_pages=None, sleep_between=SLEEP_BETWEEN_PAGES):
    page = 0
    pages_fetched = 0
    seen_ids = set()

    while True:
        if max_pages is not None and pages_fetched >= max_pages:
            break
        payload = dict(payload_base)
        payload['page'] = page
        resp_json = post_with_retries(api_url, payload, headers)
        data = resp_json.get('data', {}) if isinstance(resp_json, dict) else {}
        rows = data.get('rows', []) if isinstance(data, dict) else []
        if not rows:
            break
        df_page = page_to_dataframe(data)
        # optional dedupe similar to above...
        yield df_page
        pages_fetched += 1
        page += 1
        has_more = data.get('hasMore', None)
        if has_more is False:
            break
        if has_more is None and len(rows) < payload_base.get('pageSize', 100):
            break
        time.sleep(sleep_between)

# Example consumption:
for page_df in stream_pages_as_dataframes(API_URL, HEADERS, payload_base):
    print("Got page with", len(page_df), "rows")
    # process page_df here (e.g., save to file, transform, etc.)


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



#cur.close() 

#ctx.close() 
 


# COMMAND ----------

display(df_all)

# COMMAND ----------


write_pandas(
    snflk_conn,
    df_all,
    "TSENTIMENT_SPRINKLR_RAW",
    auto_create_table=True
)
