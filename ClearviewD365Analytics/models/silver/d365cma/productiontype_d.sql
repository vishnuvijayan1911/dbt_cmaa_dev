{{ config(materialized='table', tags=['silver'], alias='productiontype') }}

WITH detail AS (
    SELECT e1.EnumValueID AS ProductionTypeID
         , e1.EnumValue   AS ProductionType
      FROM {{ ref('enumeration') }} e1
     WHERE e1.Enum = 'ProdType'
)

SELECT ProductionTypeID
     , ProductionType
  FROM detail
 ORDER BY ProductionTypeID;
