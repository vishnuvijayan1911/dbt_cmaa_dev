{{ config(materialized='table', tags=['silver'], alias='productiontype') }}

WITH detail AS (
    SELECT e1.EnumValueID AS ProductionTypeID
         , e1.EnumValue   AS ProductionType
      FROM {{ ref('enumeration') }} e1
     WHERE e1.Enum = 'ProdType'
)

SELECT ProductionTypeID
     , ProductionType
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
  FROM detail
 ORDER BY ProductionTypeID;
