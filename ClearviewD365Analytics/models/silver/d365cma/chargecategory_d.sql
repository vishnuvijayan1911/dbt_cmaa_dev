{{ config(materialized='table', tags=['silver'], alias='chargecategory') }}

WITH detail AS (
    SELECT
        we1.enumid AS ChargeCategoryID,
        we1.enumvalue   AS ChargeCategory
      FROM {{ ref('enumeration') }} AS we1
     WHERE we1.enum = 'markupcategory'
)

SELECT *
    , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
    , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
  FROM detail;
