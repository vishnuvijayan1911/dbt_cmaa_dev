{{ config(materialized='table', tags=['silver'], alias='chargecategory') }}

SELECT *
  FROM {{ ref('chargecategory_d') }};
