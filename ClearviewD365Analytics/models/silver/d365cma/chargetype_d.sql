{{ config(materialized='table', tags=['silver'], alias='chargetype') }}

SELECT *
  FROM {{ ref('chargetype_d') }};
