{{ config(materialized='table', tags=['silver'], alias='chargetype') }}

WITH detail AS (
    SELECT
        we.enumvalueid AS ChargeTypeID,
        we.enumvalue   AS ChargeType
      FROM {{ ref('enumeration') }} AS we
     WHERE we.enum = 'MarkUpType'
)

SELECT *
  FROM detail;
