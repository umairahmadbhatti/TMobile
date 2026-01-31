BRONZE_TABLE = "BDM_PPDA_DB.PROD_BOARD_T.TSENTIMENT_SPRINKLR_BRONZE"
SILVER_TABLE = "BDM_PPDA_DB.PROD_BOARD_T.TSENTIMENT_SPRINKLR_SILVER"
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
query = f"""
WITH bronze_base AS (
  SELECT DISTINCT
      ID::STRING                    AS BRONZE_ID,
      UNIVERSAL_MESSAGE_ID::STRING  AS UMID, 
      MESSAGE_CONTENT::STRING       AS MESSAGE_CONTENT,
      PLATFORM::STRING              AS PLATFORM,
      LOAD_DATE::DATE               AS LOAD_DATE
  FROM {BRONZE_TABLE}
  WHERE ID IS NOT NULL
    AND ID::STRING <> ''
    AND MESSAGE_CONTENT NOT RLIKE '^[0-9A-Fa-f]{32,64}$'
    --AND SN_CREATED_TIME_PST = '2025-10-28'
),
silver_umids AS (
  SELECT DISTINCT b.UMID
  FROM {SILVER_TABLE} s
  JOIN bronze_base b
    ON b.BRONZE_ID = s.BRONZE_ID::STRING
),
dedup_bronze AS (
  SELECT *
  FROM (
    SELECT
      b.*,
      ROW_NUMBER() OVER (
        PARTITION BY b.UMID
        ORDER BY b.LOAD_DATE DESC, b.BRONZE_ID DESC
      ) AS rn
    FROM bronze_base b
  )
  WHERE rn = 1
)
SELECT TOP 1750
  d.BRONZE_ID,
  d.UMID AS UNIVERSAL_MESSAGE_ID,
  d.MESSAGE_CONTENT,
  d.PLATFORM
FROM dedup_bronze d
LEFT JOIN silver_umids s
  ON d.UMID = s.UMID
WHERE s.UMID IS NULL
AND d.MESSAGE_CONTENT NOT RLIKE '^[0-9A-Fa-f]{32,64}$'
AND MESSAGE_CONTENT IS NOT NULL
AND TRIM(MESSAGE_CONTENT) <> ''
AND MESSAGE_CONTENT NOT RLIKE '[A-Za-z0-9]'
ORDER BY d.LOAD_DATE DESC;
"""

cur.execute(query)
df_pd = cur.fetch_pandas_all()

query_topics = f"""
SELECT * FROM BDM_PPDA_DB.PROD_BOARD_T.EXEC_DASHBOARD_CATEGORIZATION
"""
cur.execute(query_topics)
topics_df_pd = cur.fetch_pandas_all()

print(f"Fetched {len(df_pd.index)} rows from {BRONZE_TABLE}.")
