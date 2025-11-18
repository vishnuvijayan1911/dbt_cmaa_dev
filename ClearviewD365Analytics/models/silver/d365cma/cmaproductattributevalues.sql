{{ config(materialized='table', tags=['silver'], alias='cmaproductattributevalues') }}

-- TODO: replace this pass-through with curated logic once the dimensional shape is finalized.
SELECT *
  FROM silver.cmaproductattributevalues;
