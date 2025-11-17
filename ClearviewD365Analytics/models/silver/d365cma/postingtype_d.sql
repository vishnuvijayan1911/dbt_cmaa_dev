{{ config(materialized='table', tags=['silver'], alias='postingtype') }}

SELECT *
  FROM {{ ref('postingtype_d') }};
