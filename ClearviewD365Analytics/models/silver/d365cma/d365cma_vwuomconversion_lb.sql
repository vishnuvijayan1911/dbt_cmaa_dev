{{ config(materialized='table', tags=['silver'], alias='vwuomconversion_lb') }}

SELECT *
  FROM {{ ref('d365cma_uom_d') }};

