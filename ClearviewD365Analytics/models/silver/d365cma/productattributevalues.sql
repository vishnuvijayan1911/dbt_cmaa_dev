{{ config(materialized='table', tags=['silver'], alias='cmaproductattributevalues') }}

-- TODO: replace this pass-through with curated logic once the dimensional shape is finalized.
SELECT *
    , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
    , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
  FROM silver.cmaproductattributevalues;
