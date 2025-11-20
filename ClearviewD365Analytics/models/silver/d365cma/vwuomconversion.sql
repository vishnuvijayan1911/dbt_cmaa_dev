{{ config(materialized='table', tags=['silver'], alias='vwuomconversion') }}

SELECT *
  FROM {{ ref('uom_d') }};

