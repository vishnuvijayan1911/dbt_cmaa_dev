{{ config(materialized='table', tags=['silver'], alias='vwuomconversion_pc') }}

SELECT *
  FROM {{ ref('vwuomconversion_pc') }};

