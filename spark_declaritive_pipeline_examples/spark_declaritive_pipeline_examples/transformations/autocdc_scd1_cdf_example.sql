-- ============================================================
-- BRONZE: read in CDF from source requests table and filter for branch
-- ============================================================

CREATE OR REFRESH STREAMING TABLE requests_bronze
COMMENT "Bronze layer: Raw requests with parsed branch attribute"
TBLPROPERTIES ("quality" = "bronze")
AS
WITH parsed AS (
  SELECT
    _id,
    requestedBy,
    customValues,
    ingestion_time,
    _change_type AS operation,
    _commit_version as commitversion,
    _commit_timestamp as committimestamp,
    MAP_FROM_ENTRIES(
      TRANSFORM(
        FROM_JSON(
          customValues,
          'ARRAY<STRUCT<name: STRING, value: STRING>>'
        ),
        x -> STRUCT(LOWER(x.name) AS key, x.value AS value)
      )
    ) AS attributes_map
  FROM STREAM(nethra.sdp.requests_raw) WITH (readChangeFeed=true, startingVersion=0)
)
SELECT
  _id,
  requestedBy,
  customValues,
  ingestion_time,
  attributes_map['branch'] AS branch,
  operation,
  commitversion,
  committimestamp
FROM parsed
WHERE attributes_map['branch'] IS NOT NULL;


-- ============================================================
-- SILVER: Flattened table with SCD Type 1 (Auto CDC)
-- ============================================================
CREATE OR REFRESH STREAMING TABLE requests_silver
COMMENT "Silver layer: Flattened requests with SCD Type 1 deduplication"
TBLPROPERTIES (
  "quality" = "silver"
);

CREATE FLOW silver_requests_flow_scd1 AS
AUTO CDC INTO requests_silver
FROM STREAM(requests_bronze)
KEYS (_id)
APPLY AS DELETE WHEN operation = "delete"
SEQUENCE BY committimestamp
STORED AS SCD TYPE 1;
