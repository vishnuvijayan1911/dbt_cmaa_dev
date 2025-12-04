{{ config(materialized='table', tags=['silver'], alias='cmatagactualstable') }}

-- TODO: refine this pass-through once the curated transformation is defined.
SELECT *
  FROM silver.cmatagactualstable;
