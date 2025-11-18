{{ config(materialized='table', tags=['silver'], alias='inventsumavailablephysical') }}

-- TODO: replace this pass-through with curated logic once the required granularity is confirmed.
SELECT *
  FROM silver.inventsumavailablephysical;
