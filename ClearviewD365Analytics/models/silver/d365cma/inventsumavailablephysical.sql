{{ config(materialized='table', tags=['silver'], alias='inventsumavailablephysical') }}

-- TODO: replace this pass-through with curated logic once the required granularity is confirmed.
SELECT *
    , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
    , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
  FROM silver.inventsumavailablephysical;
