{{ config(materialized='table', tags=['silver'], alias='mainaccount') }}

-- Simple pass-through dimension for main account hierarchy.
SELECT *
    , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
    , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
  FROM {{ ref('mainaccount') }};
