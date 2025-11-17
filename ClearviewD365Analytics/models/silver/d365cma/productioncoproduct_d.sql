{{ config(materialized='table', tags=['silver'], alias='productioncoproduct') }}

-- Source file: cma/cma/layers/_base/_silver/productioncoproduct/productioncoproduct.py
-- Root method: Productioncoproduct.productioncoproductdetail [ProductionCoProductDetail]
-- Inlined methods: Productioncoproduct.productioncoproductstage [ProductionCoProductStage]
-- external_table_name: ProductionCoProductDetail
-- schema_name: temp

WITH
productioncoproductstage AS (
    SELECT pr.cmaplanid        AS InnerDiameter
             , pr.cmaplanod        AS OuterDiameter
             , pr.inventrefid      AS ReferenceID
             , pr.inventreftype    AS ReferenceTypeID
             , pr.inventreftransid AS ReferenceLotID
             , 1                   AS _SourceID
             , pr.recid            AS _RecID

          FROM {{ ref('pmfprodcoby') }} pr
)
SELECT 
         ROW_NUMBER() OVER (ORDER BY ts._RecID, ts._SourceID) AS ProductionCoProductKey
         ,ts.InnerDiameter   AS InnerDiameter
         , ts.OuterDiameter   AS OuterDiameter
         , ts.ReferenceID     AS ReferenceID
         , ts.ReferenceTypeID AS ReferenceTypeID
         , e1.enumvalue       AS ReferenceType
         , ts.ReferenceLotID  AS ReferenceLotID
         , ts._SourceID       AS _SourceID
         , ts._RecID          AS _RecID
         ,CURRENT_TIMESTAMP                                               AS _CreatedDate
        , CURRENT_TIMESTAMP                                               AS _ModifiedDate

     FROM productioncoproductstage               ts
      LEFT JOIN {{ ref('enumeration') }} e1
        ON e1.enum        = 'InventRefType'
       AND e1.enumvalueid = ts.ReferenceTypeID;

