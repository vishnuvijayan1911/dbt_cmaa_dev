{{ config(materialized='table', tags=['silver'], alias='vwuomconversion_ft') }}

SELECT *
  FROM {{ ref('d365cma_uom_d') }};

