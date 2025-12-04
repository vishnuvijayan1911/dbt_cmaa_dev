{{ config(materialized='table', tags=['silver'], alias='vwuomconversion') }}

SELECT *
  FROM {{ ref('d365cma_uom_d') }};

