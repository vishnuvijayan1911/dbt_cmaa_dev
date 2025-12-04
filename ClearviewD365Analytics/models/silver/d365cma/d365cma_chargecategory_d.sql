{{ config(materialized='table', tags=['silver'], alias='chargecategory') }}

WITH detail AS (
    SELECT
        we1.enumid AS ChargeCategoryID,
        we1.enumvalue   AS ChargeCategory
      FROM {{ ref('enumeration') }} AS we1
     WHERE we1.enum = 'markupcategory'
)

SELECT *
  FROM detail;
