{{ config(materialized='table', tags=['silver'], alias='chargetype') }}

WITH detail AS (
    SELECT
        we.enumid AS ChargeTypeID,
        we.enumvalue   AS ChargeType
      FROM {{ ref('enumeration') }} AS we
     WHERE we.enum = 'cmamarkuptype'
)

SELECT *
  FROM detail
