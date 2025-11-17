{{ config(materialized='table', tags=['silver'], alias='mainaccount') }}

-- Simple pass-through dimension for main account hierarchy.
SELECT *
  FROM silver.cma_MainAccount;
