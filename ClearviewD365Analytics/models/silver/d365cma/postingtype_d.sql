{{ config(materialized='table', tags=['silver'], alias='postingtype') }}

WITH detail AS (
    SELECT
        e1.EnumValueID AS PostingTypeID,
        e1.EnumValue   AS PostingType
      FROM {{ ref('enumeration') }} AS e1
     WHERE e1.Enum = 'LedgerPostingType'
)

SELECT *
  FROM detail;
