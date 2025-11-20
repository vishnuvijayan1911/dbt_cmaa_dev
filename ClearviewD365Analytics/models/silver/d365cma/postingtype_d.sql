{{ config(materialized='table', tags=['silver'], alias='postingtype') }}
<<<<<<< HEAD
=======

>>>>>>> 35ccc5120bfee1b1bfe3164e93c01b96ca8df4ca
WITH detail AS (
    SELECT
        e1.enumvalueid AS PostingTypeID,
        e1.enumvalue   AS PostingType
      FROM {{ ref('enumeration') }} AS e1
     WHERE e1.enum = 'LedgerPostingType'
)

SELECT *
  FROM detail;