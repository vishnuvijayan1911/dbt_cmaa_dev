{{ config(materialized='table', tags=['silver'], alias='cmatagcostsbycostgroup') }}

-- TODO: refine this pass-through once the curated transformation is defined.
SELECT *
  FROM silver.cmatagcostsbycostgroup;
