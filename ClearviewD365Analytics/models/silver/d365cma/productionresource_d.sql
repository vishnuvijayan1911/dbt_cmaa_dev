{{ config(materialized='table', tags=['silver'], alias='productionresource_dim') }}

-- Source file: cma/cma/layers/_base/_silver/productionresource/productionresource.py
-- Root method: Productionresource.productionresourcedetail [ProductionResourceDetail]
-- Inlined methods: Productionresource.productionresourcestage [ProductionResourceStage]
-- external_table_name: ProductionResourceDetail
-- schema_name: temp

WITH
productionresourcestage AS (
    SELECT x.LegalEntityID
             , x.ResourceID
             , x.Resource
             , x.ResourceGroupID
             , x.ResourceGroup
             , x.ResourceTypeID
             , x._SourceID
             , x._RecID
             , x.Latest


          FROM (   SELECT wc.dataareaid  AS LegalEntityID
                        , wc.wrkctrid    AS ResourceID
                        , wc.name        AS Resource
                        , wcrg.wrkctrid  AS ResourceGroupID
                        , wc1.name       AS ResourceGroup
                        , wc.wrkctrtype  AS ResourceTypeID
                        , 1              AS _SourceID
                        , wc.recid       AS _RecID
                        , ROW_NUMBER() OVER (PARTITION BY wc.recid
    ORDER BY wcrgr.validto DESC)         AS Latest

                     FROM {{ ref('wrkctrtable') }}                     wc
                     LEFT JOIN {{ ref('wrkctrresourcegroupresource') }} wcrgr
                       ON wc.dataareaid    = wcrgr.dataareaid
                      AND wcrgr.wrkctrid   = wc.wrkctrid
                     LEFT JOIN {{ ref('wrkctrresourcegroup') }}         wcrg
                       ON wcrg.dataareaid  = wcrgr.dataareaid
                      AND wcrg.recid       = wcrgr.resourcegroup
                     LEFT JOIN  {{ ref('wrkctrtable') }}             wc1
                       ON wc1.wrkctrid     = wcrg.wrkctrid) x
         WHERE x.Latest = 1;
)
SELECT ROW_NUMBER() OVER (ORDER BY ts._RecID, ts._SourceID) AS ProductionResourceKey,
    ts.LegalEntityID                                                                  AS LegalEntityID
   , ts.ResourceID                                                                     AS ResourceID
   , CASE WHEN ts.Resource = '' THEN ts.ResourceID ELSE ts.Resource END                AS Resource
   , ts.ResourceTypeID                                                                 AS ResourceTypeID
   , e1.enumvalue                                                                      AS ResourceType
   , ts.ResourceGroupID                                                                AS ResourceGroupID
   , CASE WHEN ts.ResourceGroup = '' THEN ts.ResourceGroupID ELSE ts.ResourceGroup END AS ResourceGroup
   , ts._SourceID                                                                      AS _SourceID
   , ts._RecID                                                                         AS _RecID

FROM productionresourcestage               ts
LEFT JOIN {{ ref('enumeration') }} e1
  ON e1.enum        = 'WrkCtrType'
 AND e1.enumvalueid = ts.ResourceTypeID
