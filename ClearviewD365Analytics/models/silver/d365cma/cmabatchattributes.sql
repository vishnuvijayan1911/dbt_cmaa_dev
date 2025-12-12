{{ config(materialized='table', tags=['silver'], alias='cmabatchattributes') }}

-- TODO: replace this pass-through with curated logic once source requirements are defined.
SELECT *
    , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
    , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
  FROM silver.cmabatchattributes;
