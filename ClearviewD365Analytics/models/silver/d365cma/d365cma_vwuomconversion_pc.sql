{{ config(materialized='table', tags=['silver'], alias='vwuomconversion_pc') }}

SELECT *
  FROM {{ ref('d365cma_uom_d') }};

