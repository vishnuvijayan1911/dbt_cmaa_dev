{{ config(materialized='table', tags=['silver'], alias='bom') }}

-- Source file: cma/cma/layers/_base/_silver/bom/bom.py
-- Root method: Bom.bomdetail [BOMDetail]
-- Inlined methods: Bom.bomunit [BOMUnit]
-- external_table_name: BOMDetail
-- schema_name: temp

WITH
bomunit AS (
    SELECT bv.recid   AS RecID
             , itm.unitid AS UnitID

          FROM {{ ref('bomversion') }}             bv
         INNER JOIN {{ ref('inventtable') }}       it
            ON it.dataareaid   = bv.dataareaid
           AND it.itemid       = bv.itemid
         INNER JOIN {{ ref('inventtablemodule') }} itm
            ON itm.dataareaid  = it.dataareaid
           AND itm.itemid      = it.itemid
         WHERE itm.moduletype = 0;
)
SELECT t.BOMkey
          ,t.LegalEntityID
         , t.BOMID
         , t.BOM
         , t.FormulaSize
         , t.FormulaUOM
         , t.Yield
         , t._RecID
         , t._SourceID  
        ,CURRENT_TIMESTAMP                                               AS _CreatedDate
        , CURRENT_TIMESTAMP                                               AS _ModifiedDate

      FROM   (SELECT ROW_NUMBER() OVER (ORDER BY bv.recid) AS BOMKey
                     ,bv.dataareaid     AS LegalEntityID
                    , bv.bomid          AS BOMID
                    , bv.name           AS BOM
                    , bv.pmfbatchsize   AS FormulaSize
                    , tu.UnitID         AS FormulaUOM
                    , bv.pmfyieldpct    AS Yield
                    , bv.recid          AS _RecID
                    , 1                 AS _SourceID
                    , ROW_NUMBER() OVER (PARTITION BY bv.bomid, bv.dataareaid
ORDER BY bv.bomid, bv.dataareaid DESC) AS RankValue
                 FROM {{ ref('bomversion') }} bv
                 LEFT JOIN bomunit     tu
                   ON tu.RECID  = bv.recid
                WHERE bv.active = 1) AS t
     WHERE RankValue = 1;

