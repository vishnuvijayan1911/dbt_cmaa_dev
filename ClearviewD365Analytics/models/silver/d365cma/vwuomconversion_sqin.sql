{{ config(materialized='table', tags=['silver'], alias='vwuomconversion_sqin') }}

SELECT *
  FROM {{ ref('uom_d') }};

