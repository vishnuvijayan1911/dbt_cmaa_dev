{{ config(materialized='table', tags=['silver'], alias='inventorymakeorbuy') }}

SELECT *
  FROM {{ ref('inventorymakeorbuy_d') }};
