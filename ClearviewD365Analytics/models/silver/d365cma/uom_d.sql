{{ config(materialized='table', tags=['silver'], alias='uom') }}

-- Source file: cma/cma/layers/_base/_silver/uom/uom.py
-- Root method: Uom.uomdetail [UOMDetail]
-- Inlined methods: Uom.uomstage [UOMStage]
-- external_table_name: UOMDetail
-- schema_name: temp

WITH
uomstage AS (
    SELECT uom.symbol      AS UOM
              , uom.unitofmeasureclass AS UnitOfMeasureClass
              , uom.recid             AS _RecID
              , 1                      AS _SourceID
            FROM {{ ref('unitofmeasure') }} uom
)
SELECT 
     ROW_NUMBER() OVER (ORDER BY ts._RecID, ts._SourceID) AS UOMKey,
    ts.UOM        AS UOM
         , we1.enumvalue AS UOMClass
         , ts._RecID     AS _RecID
         , ts._SourceID  AS _SourceID
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                                            AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                                            AS _ModifiedDate
      FROM uomstage               ts
      LEFT JOIN {{ ref('enumeration') }} we1
        ON we1.enum        = 'UnitOfMeasureClass'
       AND we1.enumvalueid = ts.UnitOfMeasureClass;

