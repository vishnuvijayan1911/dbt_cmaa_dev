{{ config(materialized='table', tags=['silver'], alias='vwuomconversion_lb') }}

SELECT *
  FROM {{ ref('vwuomconversion_lb') }};

