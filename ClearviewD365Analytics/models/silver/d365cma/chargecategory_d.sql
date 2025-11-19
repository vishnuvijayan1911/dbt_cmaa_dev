{{ config(materialized='table', tags=['silver'], alias='chargecategory') }}

WITH detail AS (
    SELECT
        we1.EnumValueID AS ChargeCategoryID,
        we1.EnumValue   AS ChargeCategory
      FROM {{ ref('enumeration') }} AS we1
     WHERE we1.Enum = 'MarkupCategory'
)

SELECT *
  FROM detail;
