{{ config(materialized='table', tags=['silver'], alias='costgroup_source') }}

-- TODO: replace this pass-through with curated logic once the cost group dimensional model is built.
SELECT *
    , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
    , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
  FROM silver.costgroup;
