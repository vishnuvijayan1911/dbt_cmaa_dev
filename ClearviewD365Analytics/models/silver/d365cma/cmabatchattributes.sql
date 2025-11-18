{{ config(materialized='table', tags=['silver'], alias='cmabatchattributes') }}

-- TODO: replace this pass-through with curated logic once source requirements are defined.
SELECT *
  FROM silver.cmabatchattributes;
