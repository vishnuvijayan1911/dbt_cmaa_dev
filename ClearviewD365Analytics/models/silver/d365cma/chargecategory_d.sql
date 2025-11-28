{{ config(materialized='table', tags=['silver'], alias='chargecategory') }}

WITH detail AS (
    SELECT
        we1.enumvalueid AS ChargeCategoryID,
        we1.enumvalue   AS ChargeCategory
      FROM {{ ref('enumeration') }} AS we1
     WHERE we1.enum = 'MarkupCategory'
)

SELECT *
  FROM detail;
