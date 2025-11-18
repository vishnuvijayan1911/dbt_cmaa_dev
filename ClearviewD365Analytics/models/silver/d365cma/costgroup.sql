{{ config(materialized='table', tags=['silver'], alias='costgroup_source') }}

-- TODO: replace this pass-through with curated logic once the cost group dimensional model is built.
SELECT *
  FROM silver.costgroup;
