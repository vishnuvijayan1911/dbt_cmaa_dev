{{ config(materialized='table', tags=['silver'], alias='vwuomconversion_sqin') }}

SELECT *
  FROM {{ ref('d365cma_uom_d') }};

