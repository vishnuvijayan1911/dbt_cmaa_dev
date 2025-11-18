{{ config(materialized='table', tags=['silver'], alias='vwuomconversion_ft') }}

SELECT *
  FROM {{ ref('uom_d') }};

