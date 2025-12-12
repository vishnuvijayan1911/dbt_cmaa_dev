{{ config(materialized='table', tags=['silver'], alias='cmatagactualstable') }}

-- TODO: refine this pass-through once the curated transformation is defined.
SELECT *
    , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
    , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
  FROM silver.cmatagactualstable;
