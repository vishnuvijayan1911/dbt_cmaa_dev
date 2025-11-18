{{ config(materialized='table', tags=['silver'], alias='vwuomconversion_lb') }}

SELECT *
  FROM {{ ref('uom_d') }};

