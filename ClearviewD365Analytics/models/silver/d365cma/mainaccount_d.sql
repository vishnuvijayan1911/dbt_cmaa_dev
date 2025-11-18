{{ config(materialized='table', tags=['silver'], alias='mainaccount') }}

-- Simple pass-through dimension for main account hierarchy.
SELECT *
  FROM {{ ref('cma_mainaccount_d') }};
