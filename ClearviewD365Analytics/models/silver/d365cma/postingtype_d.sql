{{ config(materialized='table', tags=['silver'], alias='postingtype') }}

WITH detail AS (
    SELECT
        e1.enumid AS PostingTypeID,
        e1.enumvalue   AS PostingType
      FROM {{ ref('enumeration') }} AS e1
     WHERE e1.enum = 'postingtype'
)

SELECT *
  FROM detail;