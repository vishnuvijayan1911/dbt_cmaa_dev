{{ config(materialized='table', tags=['silver'], alias='vwuomconversion_pc') }}

SELECT *
  FROM {{ ref('uom_d') }};

