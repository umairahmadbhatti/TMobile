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
    schema = '.PROD_BOARD_T', 
    token=access_token 
)    

cur = ctx.cursor() 

query = '''SELECT DATE_PART(
         epoch_millisecond,
         CONVERT_TIMEZONE('UTC',
           DATEADD(millisecond, 1, MAX(TRY_TO_TIMESTAMP_TZ(SN_CREATED_TIME)))
         )
       )::NUMBER(38,0) AS LASTMODIFIEDDATE
FROM BDM_PPDA_DB.PROD_BOARD_T.TSENTIMENT_SPRINKLR_BRONZE;
''' 
cur.execute(query) 
data=pd.DataFrame.from_records(iter(cur), columns=[x[0] for x in cur.description]) 
#print(tabulate(data, headers='keys', tablefmt='psql')) 
startTime_ms  = data.LASTMODIFIEDDATE[0]
print(startTime_ms)

# Close connections 
#cur.close() 
#ctx.close() 

# COMMAND ----------

from datetime import datetime
from zoneinfo import ZoneInfo  # Use `pytz` if on Python < 3.9

# Get current time in PST
pst_now = datetime.now(ZoneInfo("America/Los_Angeles"))

# Convert to microseconds since Unix epoch
end_time_ms = int(pst_now.timestamp() * 1_000)
print("Microseconds since epoch:", end_time_ms)


# COMMAND ----------

#startTime_ms = '1759276800000'
#end_time_ms = '1752735599999'
print(startTime_ms)
print(end_time_ms)

# COMMAND ----------

# MAGIC %md
# MAGIC 1758524400001
# MAGIC 1758550906182

# COMMAND ----------

# Databricks single cell: Parameterized topics → build Spark DataFrame ONLY (no save)
# - START_AT accepts either a 'YYYY-MM-DD' string (LA midnight) OR an epoch (seconds or milliseconds; Python/numpy/pandas ok)
# - Matches the provided payload (LA timezone, DATE_HISTOGRAM 1d on SN_CREATED_TIME, includes ACCOUNT_ID)
# - dt is a UTC ISO-8601 string (e.g., 2025-09-18T07:00:00Z) for Power BI
# - DATE_HISTOGRAM buckets SN_CREATED_TIME to the start of each LA day; converting to UTC shifts to 07:00/08:00Z depending on DST.

# =========================
# ======== PARAMS =========
# =========================
TOPIC_IDS = [
    "687a9556b903ed6d8880c935",  # [OCEO] Srini Gopalan
    "687a94a0b903ed6d887fc582",  # [OCEO] Mike Sievert 2
    "6629305d0e208512f2efc310",  # [CI] T-Mobile Brand Monitoring
    "687a984bb903ed6d88850558",
    "687a96b1b903ed6d8882bc3a",
    "687a988eb903ed6d888592b1",
    "687a98fcb903ed6d888628d1",
    "687a9789b903ed6d8883ee31",
    "687a9704b903ed6d88832fe3",
    "687a974eb903ed6d888398d7",
    "687a97b7b903ed6d88842cb9",
    "687a98cfb903ed6d8885eb71",
    "687a9660b903ed6d88824e04",
    "687a9967b903ed6d8886c4b3",
    "687a9819b903ed6d8884bb48",
    "687a992ab903ed6d88866ba6",
    "68be226440467b53dafde5f5",
    "68ed611c5b45201514cf72d8",  #Thelayoff
    "68d1d0a725a8782101255fef", # subreddit
    "68e40513db535a37680efe9d",
    "66216770a83a8771b7d34f9e",
    "662131f2a83a8771b7769e39"
]
RUN_MODE = "full"  # "full" or "incremental"

# Accepts 'YYYY-MM-DD' string OR epoch (seconds or milliseconds). If you already defined startTime_ms elsewhere, this will pick it up.
# Robustly coalesce if a widget or prior cell set startTime_ms to None / "" / "none" / "null" / "nan".
_DEFAULT_START_AT = startTime_ms
_raw_start_at = globals().get("startTime_ms", _DEFAULT_START_AT)
if (_raw_start_at is None) or (isinstance(_raw_start_at, str) and _raw_start_at.strip().lower() in ("", "none", "null", "nan")):
    START_AT = _DEFAULT_START_AT
else:
    START_AT = _raw_start_at  # may be date string or epoch-like number/string

CHUNK_DAYS = 1                 # time window per API call (helps stability)
PAGE_SIZE = 1000               # rows per page to match your payload
REQUEST_PAUSE_SEC = 0.15       # small pause between pages
TIMEZONE = "America/Los_Angeles"  # as in payload

# Optional path only used to discover watermark in incremental mode (no writes occur)
ADLS_DELTA_PATH = "abfss://oceo-analytics@prdedsnpdwu2adls1.dfs.core.windows.net/T-Sentiment"

# =========================
# ====== CONSTANTS ========
# =========================
import json, requests, re, time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple

from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType, DoubleType
from pyspark.sql import Row
from pyspark.sql import functions as F

try:
    from zoneinfo import ZoneInfo
    LA_TZ = ZoneInfo(TIMEZONE)
except Exception:
    import pytz
    LA_TZ = pytz.timezone(TIMEZONE)

# Use UTC in Spark session so timestamps are stable when parsed/serialized
spark.conf.set("spark.sql.session.timeZone", "UTC")

URL = "https://api2.sprinklr.com/prod2/api/v2/reports/query"

# ---- Hard-coded headers (swap later) ----
HEADERS = {
    'Authorization': 'Bearer UTdKJf+rtZuU5fPnGF6W0IWGVnbSkXEGfDoOdELjzsMwM2ZmY2IyMS1mNmM3LTM2OGYtOTM5ZS1mMjI3ODA5ODdhZTg=', # USE ACCESS TOKEN (NOT REFRESH TOKEN) from Auth Flow
    'cache-control': 'no-cache',
    'key': '4h7ruvbfxhcsaa5w9z9rches',
    "Content-Type": "application/json",
}

# =========================
# ====== UTILITIES ========
# =========================
def epoch_to_ms(x) -> int:
    """
    Convert epoch seconds or epoch milliseconds to milliseconds.
    Accepts Python ints/floats, numpy scalar ints/floats, pandas scalars/Timestamps, or digit strings.
    """
    if x is None:
        return None

    # Unwrap numpy/pandas scalars (e.g., np.int64 → int)
    try:
        if hasattr(x, "item"):
            x = x.item()
    except Exception:
        pass

    # Handle pandas Timestamp-like objects (ns since epoch)
    try:
        if hasattr(x, "value") and isinstance(x.value, int):
            return int(x.value // 1_000_000)  # ns → ms
    except Exception:
        pass

    # Disallow bools (subclass of int)
    if isinstance(x, bool):
        raise ValueError("START_AT cannot be a boolean.")

    # Digit strings
    if isinstance(x, str):
        s = x.strip()
        if re.fullmatch(r"\d{10,13}", s):
            v = int(s)
            return v if v >= 1_000_000_000_000 else v * 1000
        # Not digits → let caller handle as date, not here
        raise ValueError(f"Epoch-like START_AT must be 10–13 digit integer (sec/ms). Got: {x!r}")

    # Numeric (covers Python + numpy numbers)
    import numbers as _numbers
    if isinstance(x, _numbers.Number):
        v = int(x)
        return v if v >= 1_000_000_000_000 else v * 1000

    # Last-ditch coercion
    try:
        v = int(x)
        return v if v >= 1_000_000_000_000 else v * 1000
    except Exception:
        raise ValueError(f"Unsupported epoch-like START_AT type: {type(x)} (value={x!r})")

def to_epoch_ms(x) -> int:
    """Return epoch milliseconds from datetime-like or epoch-like input."""
    if x is None:
        return None
    if isinstance(x, datetime):
        dt = x if x.tzinfo is not None else x.replace(tzinfo=LA_TZ)
        return int(dt.astimezone(timezone.utc).timestamp() * 1000)
    # Delegate numeric/string epochs to epoch_to_ms
    try:
        return epoch_to_ms(x)
    except Exception:
        pass
    # Try date string 'YYYY-MM-DD'
    if isinstance(x, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", x.strip()):
        y, m, d = map(int, x.strip().split("-"))
        dt = datetime(y, m, d, 0, 0, 0, tzinfo=LA_TZ)
        return int(dt.astimezone(timezone.utc).timestamp() * 1000)
    raise ValueError(f"Unsupported START_AT type: {type(x)} (value={x!r})")

def start_param_to_ms(x) -> int:
    """
    Accepts either:
      - 'YYYY-MM-DD' string (interpreted at LA midnight), or
      - epoch seconds/milliseconds (int/str, incl. numpy/pandas scalars)
    Returns epoch ms.
    """
    if isinstance(x, str):
        s = x.strip()
        # date string
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
            return to_epoch_ms(s)
        # epoch string
        if re.fullmatch(r"\d{10,13}", s):
            return epoch_to_ms(s)
        raise ValueError(f"START_AT string must be 'YYYY-MM-DD' or 10–13 digit epoch. Got: {x!r}")
    # Non-string: try epoch first, then datetime
    try:
        val = epoch_to_ms(x)
        if val is not None:
            return val
    except Exception:
        pass
    return to_epoch_ms(x)

def looks_jsonish(s: Any) -> bool:
    if not isinstance(s, str): return False
    st = s.strip()
    return (st.startswith("{") and st.endswith("}")) or (st.startswith("[") and st.endswith("]"))

def try_parse_jsonish(x: Any) -> Any:
    if looks_jsonish(x):
        try:
            return json.loads(x)
        except Exception:
            return x
    return x

def _to_utc_z(dt_obj: datetime) -> str:
    return dt_obj.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def extract_histogram_epoch_ms(v):
    """Return epoch ms from Sprinklr DATE_HISTOGRAM objects or digit-like inputs; else None."""
    v = try_parse_jsonish(v)
    # dict from histogram
    if isinstance(v, dict):
        for k in ("key", "from", "start", "time", "value"):
            if k in v:
                try:
                    f = float(v[k])
                    if f >= 1e12:    # ms
                        return int(f)
                    if f >= 1e9:     # sec
                        return int(f * 1000)
                except Exception:
                    pass
        return None
    # digits-as-string or number
    if isinstance(v, (int, float)) or (isinstance(v, str) and v.strip().isdigit()):
        try:
            f = float(v)
            if f >= 1e12: return int(f)          # ms
            if f >= 1e9:  return int(f * 1000)   # sec
        except Exception:
            pass
    return None

def normalize_sn_created_time(v: Any) -> str:
    """
    Normalizes SN_CREATED_TIME values to UTC ISO-8601 'Z' strings.
    Handles:
      - epoch ms / s
      - strings containing epoch or ISO (with or without offset)
      - histogram objects (key/from/start/time/value)
    """
    v = try_parse_jsonish(v)

    # Unwrap dicts from DATE_HISTOGRAM rows
    if isinstance(v, dict):
        for k in ("key", "from", "start", "time", "value"):
            if k in v:
                return normalize_sn_created_time(v[k])
        return None

    # Numeric epoch
    try:
        f = float(v)
        if f > 1e12:   # ms
            return _to_utc_z(datetime.fromtimestamp(f/1000, LA_TZ))
        if f > 1e9:    # s
            return _to_utc_z(datetime.fromtimestamp(f, LA_TZ))
    except Exception:
        pass

    # String handling
    if isinstance(v, str):
        s = v.strip()
        # Pure digits => epoch
        if re.fullmatch(r"\d{10,13}", s):
            return normalize_sn_created_time(float(s))
        # ISO-like
        try:
            s2 = s.replace("Z", "+00:00")
            dt = datetime.fromisoformat(s2)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=LA_TZ)
            return _to_utc_z(dt)
        except Exception:
            return s  # leave as-is; Spark may still parse
    return str(v) if v is not None else None

UMID_KEYS = ("universalmessageid","universal_message_id","universalmessagekey","universal_message_key","umid")

def extract_umid_and_permalink(values: List[Any]) -> Tuple[Any, Any]:
    umid = None; permalink = None
    def walk(v: Any):
        nonlocal umid, permalink
        v = try_parse_jsonish(v)
        if isinstance(v, dict):
            for k, val in v.items():
                kl = str(k).lower()
                if umid is None and kl in UMID_KEYS: umid = val
                if permalink is None and "permalink" in kl: permalink = val
                walk(val)
        elif isinstance(v, list):
            for it in v: walk(it)
    for v in values:
        walk(v)
        if umid is not None and permalink is not None:
            break
    return umid, permalink

FRIENDLY = {
    "ES_MESSAGE_ID": "message_id",
    "SEM_SENTIMENT": "sentiment",
    "TOPIC_IDS": "topic_ids",
    "MESSAGE_CONTENT": "message_content",
    "SN_MESSAGE_TYPE": "message_type",
    "FROM_SN_USER": "from_sn_user",
    "ACCOUNT_ID": "account_id",
    "SN_CREATED_TIME": "sn_created_time",
    "LISTENING_MEDIA_TYPE": "source",
    "REACH_COUNT": "reach_count",
    "MENTIONS_COUNT": "mentions_count",
    "EARNED_ENGAGEMENT": "earned_engagement",
}

def heading_to_col(h: str) -> str:
    raw = re.sub(r"_\d+$", "", h)              # drop trailing ordinal
    raw = re.sub(r"^M_SPRINKSIGHTS_", "", raw) # drop metric prefix
    return FRIENDLY.get(raw, raw).lower()

def build_payload(start_ms: int, end_ms: int, page: int, page_size: int) -> Dict[str, Any]:
    # Mirrors the structure you shared, but keeps TOPIC_IDS & pagination parametric
    return {
        "report": "SPRINKSIGHTS",
        "reportingEngine": "LISTENING",
        "timeField": None,
        "startTime": start_ms,
        "endTime": end_ms,
        "timeZone": TIMEZONE,
        "page": page,
        "pageSize": page_size,
        "filters": [
            {
                "dimensionName": "LST_SUPP_LNG",
                "filterType": "IN",
                "values": ["en"],
                "details": {
                    "uniqueId": "D_LST_SUPP_LNG",
                    "contentType": "DB_FILTER",
                    "DB_FILTER_REPORT_NAME": "SPRINKSIGHTS",
                    "OLD_DIM_NAME": "LANGUAGE",
                    "HAS_ALERT": False,
                    "nameQueryLookupSupported": "true",
                    "supportedMediaCSV": "ALL_SOURCES",
                    "displayNameForWarning": "Language",
                    "HAS_TOOLTIP_DATA": True
                }
            },
            {
                "dimensionName": "TOPIC_IDS",
                "filterType": "IN",
                "values": TOPIC_IDS,
                "details": {
                    "uniqueId": "D_TOPIC_IDS",
                    "contentType": "DB_FILTER",
                    "dF": True,
                    "DB_FILTER_REPORT_NAME": "SPRINKSIGHTS",
                    "OLD_DIM_NAME": "TOPIC",
                    "HAS_ALERT": False,
                    "nameQueryLookupSupported": "true",
                    "DRILLDOWN": False,
                    "displayNameForWarning": "Topic",
                    "EXIST_FILTER": False,
                    "HAS_TOOLTIP_DATA": True
                }
            }
        ],
        "groupBys": [
            {"heading":"ES_MESSAGE_ID_0","dimensionName":"ES_MESSAGE_ID","groupType":"FIELD","details":{},"namedFilters":None},
            {"heading":"SEM_SENTIMENT_1","dimensionName":"SEM_SENTIMENT","groupType":"FIELD","details":{},"namedFilters":None},
            {"heading":"TOPIC_IDS_2","dimensionName":"TOPIC_IDS","groupType":"FIELD","details":{},"namedFilters":None},
            {"heading":"MESSAGE_CONTENT_3","dimensionName":"MESSAGE_CONTENT","groupType":"FIELD","details":{},"namedFilters":None},
            {"heading":"SN_MESSAGE_TYPE_4","dimensionName":"SN_MESSAGE_TYPE","groupType":"FIELD","details":{},"namedFilters":None},
            {"heading":"FROM_SN_USER_5","dimensionName":"FROM_SN_USER","groupType":"FIELD","details":{},"namedFilters":None},
            {"heading":"ACCOUNT_ID_6","dimensionName":"ACCOUNT_ID","groupType":"FIELD","details":{},"namedFilters":None},
            {
                "heading":"SN_CREATED_TIME_7",
                "dimensionName":"SN_CREATED_TIME",
                "groupType":"FIELD",
                "details":{"isDateTypeDimension": True},
                "namedFilters":None
            },
            {"heading":"LISTENING_MEDIA_TYPE_8","dimensionName":"LISTENING_MEDIA_TYPE","groupType":"FIELD","details":{},"namedFilters":None},
        ],
        "projections": [
            {"heading":"M_SPRINKSIGHTS_REACH_COUNT_0","measurementName":"REACH_COUNT","aggregateFunction":"SUM","details":{}},
            {"heading":"M_SPRINKSIGHTS_MENTIONS_COUNT_1","measurementName":"MENTIONS_COUNT","aggregateFunction":"SUM","details":{}},
            {"heading":"M_SPRINKSIGHTS_EARNED_ENGAGEMENT_2","measurementName":"EARNED_ENGAGEMENT","aggregateFunction":"SUM","details":{}}
        ],
        "projectionDecorations": [],
        "projectionFilters": None,
        "sorts": None,
        "streamRequestInfo": None,
        "additional": {
            "Timezone": TIMEZONE,
            "translateResponse": "false",
            "fetchUnhealthyAccounts": "false",
            "widgetId": "68cb38c39e3fa540791980fc",
            "personaAppName": "SOCIAL_LISTENING",
            "exportInfo": "false",
            "MARGIN": "false",
            "engine": "LISTENING",
            "dashboardId": "68cb38c39e3fa54079198116",
            "showTotal": "false",
            "chartType": "POST_CARD",
            "TABULAR": "true"
        },
        "skipResolve": False,
        "jsonResponse": False
    }

def fetch_rows_for_window(start_ms: int, end_ms: int) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    page = 0
    while True:
        body = build_payload(start_ms, end_ms, page, PAGE_SIZE)
        resp = requests.post(URL, headers=HEADERS, data=json.dumps(body), timeout=120)
        if resp.status_code in (429, 500, 502, 503, 504):
            time.sleep(1.0)
            resp = requests.post(URL, headers=HEADERS, data=json.dumps(body), timeout=120)
        if resp.status_code >= 400:
            raise RuntimeError(f"HTTP {resp.status_code} – {resp.text[:800]}")
        payload = resp.json()
        data = payload.get("data") or {}
        headings = data.get("headings")
        rows = data.get("rows")
        if not isinstance(headings, list) or not isinstance(rows, list):
            break
        raw_headings = [h if isinstance(h, str) else (h.get("dimensionName") or h.get("measurementName") or h.get("heading") or f"col_{i}")
                        for i, h in enumerate(headings)]
        col_names = [heading_to_col(h) for h in raw_headings]

        page_recs = []
        for r in rows:
            if not isinstance(r, list): 
                continue
            width = min(len(col_names), len(r))
            obj = {col_names[i]: r[i] for i in range(width)}
            # normalize
            if "topic_ids" in obj and isinstance(obj["topic_ids"], str):
                obj["topic_ids"] = [p.strip() for p in re.split(r"[;,]", obj["topic_ids"]) if p.strip()]
            # histogram day → UTC ISO string
            # Removing below line as we need raw timestamp
            #obj["sn_created_time"] = normalize_sn_created_time(obj.get("sn_created_time"))
            # NEW: keep RAW epoch ms (as an integer); if not found, leave as-is
            raw_ms = extract_histogram_epoch_ms(obj.get("sn_created_time"))
            obj["sn_created_time"] = raw_ms if raw_ms is not None else obj.get("sn_created_time")

            # metrics
            for m in ("reach_count","mentions_count","earned_engagement"):
                try:
                    if obj.get(m) is not None:
                        obj[m] = float(obj[m]) if m == "earned_engagement" else int(float(obj[m]))
                except Exception:
                    pass
            # nested extras
            umid, link = extract_umid_and_permalink(list(obj.values()))
            if umid is not None: obj["universal_message_id"] = str(umid)
            if link is not None: obj["permalink"] = str(link)
            page_recs.append(obj)

        records.extend(page_recs)
        print(f"Window page {page}: +{len(page_recs)} rows (total {len(records)})")
        has_more = bool((data.get("hasMore") or data.get("has_more") or False))
        if not has_more or len(page_recs) == 0:
            break
        page += 1
        time.sleep(REQUEST_PAUSE_SEC)
    return records

def chunk_ranges(start_ms: int, end_ms: int, days: int):
    cur = start_ms
    one = timedelta(days=days)
    while cur <= end_ms:
        nxt_dt = datetime.fromtimestamp(cur/1000, LA_TZ) + one
        nxt = min(end_ms, to_epoch_ms(nxt_dt))
        yield (cur, nxt)
        cur = nxt + 1  # advance by 1 ms to avoid overlap

def path_exists(p: str) -> bool:
    try:
        dbutils.fs.ls(p)
        return True
    except Exception:
        return False

# =========================
# ====== INGESTION ========
# =========================
# End time clamp (now - 5m in LA)
now_la = datetime.now(LA_TZ)
end_ms = to_epoch_ms(now_la - timedelta(minutes=5))

# Determine start_ms (full vs incremental), robust to string date or epoch number
if RUN_MODE.lower() == "incremental" and path_exists(ADLS_DELTA_PATH):
    try:
        existing = spark.read.format("delta").load(ADLS_DELTA_PATH)
        if "sn_created_ts" in existing.columns:
            last_ts = existing.select(F.max("sn_created_ts")).collect()[0][0]
        else:
            last_ts = existing.select(F.max(F.to_timestamp("sn_created_time"))).collect()[0][0]
        start_ms = int(last_ts.timestamp() * 1000) + 1 if last_ts is not None else start_param_to_ms(START_AT)
    except Exception:
        start_ms = start_param_to_ms(START_AT)
else:
    start_ms = start_param_to_ms(START_AT)

if start_ms is None:
    # Defensive guard (shouldn't happen due to coalesce above)
    start_ms = start_param_to_ms(_DEFAULT_START_AT)

if start_ms > end_ms:
    raise ValueError(f"Computed start_ms ({start_ms}) is after end_ms ({end_ms}). Check START_AT and clock skew.")

print(f"Effective window ({TIMEZONE}): {datetime.fromtimestamp(start_ms/1000, LA_TZ).isoformat()} → {datetime.fromtimestamp(end_ms/1000, LA_TZ).isoformat()}")
print(f"Topics: {TOPIC_IDS}")

# Fetch in time chunks, accumulate
all_records: List[Dict[str, Any]] = []
for s_ms, e_ms in chunk_ranges(start_ms, end_ms, CHUNK_DAYS):
    print(f"\n=== Fetching chunk: {datetime.fromtimestamp(s_ms/1000, LA_TZ).isoformat()} → {datetime.fromtimestamp(e_ms/1000, LA_TZ).isoformat()} ===")
    chunk_records = fetch_rows_for_window(s_ms, e_ms)
    all_records.extend(chunk_records)

print(f"\nTOTAL fetched records: {len(all_records)}")

# =========================
# ===== BUILD SPARK DF ====
# =========================
def to_str_or_none(x):
    if x is None: return None
    if isinstance(x, (dict, list)):
        try: return json.dumps(x, ensure_ascii=False)
        except Exception: return str(x)
    return str(x)

def to_str_list(x):
    if x is None: return []
    if isinstance(x, list): return [str(v) for v in x if v is not None]
    if isinstance(x, str):  return [p.strip() for p in re.split(r"[;,]", x) if p.strip()]
    return [str(x)]

def to_long(x):
    try: return int(float(x)) if x is not None else None
    except Exception: return None

def to_double(x):
    try: return float(x) if x is not None else None
    except Exception: return None

schema = StructType([
    StructField("message_id",            StringType(), True),
    StructField("sentiment",             StringType(), True),
    StructField("topic_ids",             ArrayType(StringType(), True), True),
    StructField("message_content",       StringType(), True),
    StructField("message_type",          StringType(), True),
    StructField("from_sn_user",          StringType(), True),
    StructField("account_id",            StringType(), True),
    StructField("sn_created_time",       StringType(), True),  # UTC ISO-8601 'Z' string
    StructField("source",                StringType(), True),
    StructField("reach_count",           LongType(),   True),
    StructField("mentions_count",        LongType(),   True),
    StructField("earned_engagement",     DoubleType(), True),
    StructField("permalink",             StringType(), True),
    StructField("universal_message_id",  StringType(), True),
])

rows_for_spark = []
for rec in all_records:
    rows_for_spark.append(Row(
        to_str_or_none(rec.get("message_id")),
        to_str_or_none(rec.get("sentiment")),
        to_str_list(rec.get("topic_ids")),
        to_str_or_none(rec.get("message_content")),
        to_str_or_none(rec.get("message_type")),
        to_str_or_none(rec.get("from_sn_user")),
        to_str_or_none(rec.get("account_id")),
        to_str_or_none(rec.get("sn_created_time")),  # already normalized to UTC ISO 'Z'
        to_str_or_none(rec.get("source")),
        to_long(rec.get("reach_count")),
        to_long(rec.get("mentions_count")),
        to_double(rec.get("earned_engagement")),
        to_str_or_none(rec.get("permalink")),
        to_str_or_none(rec.get("universal_message_id")),
    ))

sn = F.col("sn_created_time")

# Handle 13-digit ms, 10-digit seconds, or ISO 'Z' string
sn_created_ts = (
    F.when(sn.rlike(r'^\d{13}$'), F.to_timestamp(F.from_unixtime((sn.cast("double")/1000.0))))
     .when(sn.rlike(r'^\d{10}$'), F.to_timestamp(F.from_unixtime(sn.cast("double"))))
     .otherwise(F.to_timestamp(sn, "yyyy-MM-dd'T'HH:mm:ss'Z'"))
)

sprinklr_df = spark.createDataFrame(rows_for_spark, schema=schema) \
    .withColumn("sn_created_ts", sn_created_ts) \
    .withColumn("dt", F.date_format(F.col("sn_created_ts"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))


# Parse the UTC ISO 'Z' string explicitly
#sprinklr_df = spark.createDataFrame(rows_for_spark, schema=schema) \
#    .withColumn("sn_created_ts", F.to_timestamp("sn_created_time", "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
#    .withColumn("dt", F.date_format(F.col("sn_created_ts"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))

print("DataFrame built.")
sprinklr_df.printSchema()
print(f"Row count: {sprinklr_df.count()}")

# Preview
try:
    display(sprinklr_df.orderBy(F.desc("sn_created_ts")).limit(25))
except Exception:
    sprinklr_df.orderBy(F.desc("sn_created_ts")).show(25, truncate=False)


# COMMAND ----------

from pyspark.sql.functions import (
    col,
    from_unixtime,
    to_utc_timestamp,
    to_timestamp
)

# Convert epoch ms to seconds and create UTC and PST columns
sprinklr_df = sprinklr_df.withColumn(
    "SN_CREATE_TIME_UTC",
    to_utc_timestamp(
        from_unixtime(col("sn_created_time") / 1000), "UTC"
    )
).withColumn(
    "SN_CREATE_TIME_PST",
    to_utc_timestamp(
        from_unixtime(col("sn_created_time") / 1000), "America/Los_Angeles"
    )
)

display(sprinklr_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, date_format

df = sprinklr_df


# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, from_utc_timestamp, date_format

# Convert string to timestamp (assume input is string in UTC)
df = df.withColumn(
    "SN_CREATE_TIME_UTC_ts",
    to_timestamp(col("SN_CREATE_TIME_UTC"))
)

# Convert UTC timestamp to PST (America/Los_Angeles)
df = df.withColumn(
    "SN_CREATE_TIME_PST_ts",
    from_utc_timestamp(col("SN_CREATE_TIME_UTC_ts"), "America/Los_Angeles")
)

# Format both columns as 'yyyy-MM-dd HH:mm:ss' strings
df = df.withColumn(
    "SN_CREATE_TIME_UTC",
    date_format(col("SN_CREATE_TIME_UTC_ts"), "yyyy-MM-dd HH:mm:ss")
).withColumn(
    "SN_CREATE_TIME_PST",
    date_format(col("SN_CREATE_TIME_PST_ts"), "yyyy-MM-dd HH:mm:ss")
)

display(df)

# COMMAND ----------

df.columns

# COMMAND ----------

df = df.select(
    [
        'message_id',
 'sentiment',
 'topic_ids',
 'message_content',
 'message_type',
 'from_sn_user',
 'account_id',
 'sn_created_time',
 'source',
 'reach_count',
 'mentions_count',
 'earned_engagement',
 'permalink',
 'universal_message_id',
 'sn_created_ts',
 'dt',
 'SN_CREATE_TIME_UTC',
 'SN_CREATE_TIME_PST'
    ]
)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, date_format


df = df.withColumn(
    'LOAD_DATE',
    date_format(
        current_timestamp(),
        'yyyy-MM-dd HH:mm:ss'
    )
)

display(df)

# COMMAND ----------

df.columns

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

df.columns

# COMMAND ----------

for old_name, new_name in {
    "message_id": "MESSAGE_ID",
    "sentiment": "SENTIMENT",
    "topic_ids": "TOPIC_IDS",
    "message_content": "MESSAGE_CONTENT",
    "message_type": "MESSAGE_TYPE",
    "from_sn_user": "FROM_SN_USER",
    "account_id": "ACCOUNT_ID",
    "sn_created_time": "SN_CREATED_TIME",
    "source": "SOURCE",
    "reach_count": "REACH_COUNT",
    "mentions_count": "MENTIONS_COUNT",
    "earned_engagement": "EARNED_ENGAGEMENT",
    "permalink": "PERMALINK",
    "universal_message_id": "UNIVERSAL_MESSAGE_ID",
    "sn_created_ts": "SN_CREATED_TS",
    "dt": "DT",
    "SN_CREATE_TIME_UTC": "SN_CREATED_TIME_UTC",
    "SN_CREATE_TIME_PST": "SN_CREATED_TIME_PST"
}.items():
    df = df.withColumnRenamed(old_name, new_name)

    

# COMMAND ----------

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



#cur.close() 

#ctx.close() 
 


# COMMAND ----------


write_pandas(
    snflk_conn,
    df.toPandas(),
    "TSENTIMENT_SPRINKLR_BRONZE",
    auto_create_table=True
)
