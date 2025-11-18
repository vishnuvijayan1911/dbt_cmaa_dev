{{ config(materialized='table', tags=['silver'], alias='chargetype') }}

WITH detail AS (
    SELECT
        we.EnumValueID AS ChargeTypeID,
        we.EnumValue   AS ChargeType
      FROM {{ ref('enumeration') }} AS we
     WHERE we.Enum = 'MarkUpType'
)

SELECT *
  FROM detail;
