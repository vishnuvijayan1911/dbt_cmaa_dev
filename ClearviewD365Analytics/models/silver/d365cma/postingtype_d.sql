{{ config(materialized='table', tags=['silver'], alias='postingtype') }}
WITH detail AS (
    SELECT
        e1.enumvalueid AS PostingTypeID,
        e1.enumvalue   AS PostingType
      FROM {{ ref('enumeration') }} AS e1
     WHERE e1.enum = 'LedgerPostingType'
)

SELECT *
  FROM detail;