{{ config(materialized='table', tags=['silver'], alias='salesquotelinetrans_fact') }}

-- Source file: cma/cma/layers/_base/_silver/salesquotelinetrans_f/salesquotelinetrans_f.py
-- Root method: SalesquotelinetransFact.salesquotelinetrans_factdetail [SalesQuoteLineTrans_FactDetail]
-- Inlined methods: SalesquotelinetransFact.salesquotelinetrans_factstage [SalesQuoteLineTrans_FactStage], SalesquotelinetransFact.salesquotelinetrans_factratio [SalesQuoteLineTrans_FactRatio], SalesquotelinetransFact.salesquotelinetrans_facttag [SalesQuoteLineTrans_FactTag], SalesquotelinetransFact.salesquotelinetrans_facttrans [SalesQuoteLineTrans_FactTrans], SalesquotelinetransFact.salesquotelinetrans_facttransuom [SalesQuoteLineTrans_FactTransUOM]
-- external_table_name: SalesQuoteLineTrans_FactDetail
-- schema_name: temp

WITH
salesquotelinetrans_factstage AS (
    SELECT it.recid                 AS RecID_IT
             , MAX(ql.recid)            AS RecID_QL
             , MAX(it.dataareaid)       AS DATAAREAID
             , MAX(ql.itemid)            AS ItemID
             , MAX(ql.currencycode)      AS CurrencyID
             , MAX(it.qty)               AS Qty_IT
             , MAX(it.statusissue)       AS STATUSISSUE
             , MAX(it.inventdimid)       AS INVENTDIMID
             , MAX(ql.modifieddatetime) AS _SourceDate

          FROM {{ ref('inventtrans') }}             it
         INNER JOIN {{ ref('inventtransorigin') }}  ito
            ON ito.recid       = it.inventtransorigin
         INNER JOIN {{ ref('salesquotationline') }} ql
            ON ql.dataareaid   = ito.dataareaid
           AND ql.inventtransid = ito.inventtransid
           AND ql.itemid        = ito.itemid
         WHERE it.statusissue   = 0
            OR it.statusreceipt = 0
         GROUP BY it.recid;
),
salesquotelinetrans_factratio AS (
    SELECT ts.RecID_IT                                                                                        AS RecID_IT
             , CASE WHEN SUM(-1 * ts.Qty_IT) OVER (PARTITION BY ts.RecID_QL) = 0
                    THEN 1 / CAST(ISNULL(NULLIF(COUNT(1) OVER (PARTITION BY ts.RecID_QL), 0), 1) AS FLOAT)
                    ELSE
                    CAST(ts.Qty_IT AS FLOAT) * -1
                    / CAST(ISNULL(NULLIF(SUM(-1 * ts.Qty_IT) OVER (PARTITION BY ts.RecID_QL), 0), 1) AS FLOAT)END AS PercentOfTotal

          FROM salesquotelinetrans_factstage ts;
),
salesquotelinetrans_facttag AS (
    SELECT ts.RecID_IT      AS RecID_IT
             , ib.recid        AS RecID_IB
             , id.inventbatchid AS TagID

          FROM  salesquotelinetrans_factstage              ts
         INNER JOIN {{ ref('inventdim') }}   id
            ON id.dataareaid   = ts.DATAAREAID
           AND id.inventdimid   = ts.INVENTDIMID
         INNER JOIN  {{ ref('inventbatch') }}  ib
            ON ib.dataareaid   = id.dataareaid
           AND ib.inventbatchid = id.inventbatchid
           AND ib.itemid        = ts.ItemID;
),
salesquotelinetrans_facttrans AS (
    SELECT ts.RecID_IT                                                              AS RecID_IT
             , ts.RecID_QL                                                              AS RecID_QL
             , ts.DATAAREAID                                                          AS DataAreaID
             , ts.ItemID                                                                AS ItemID
             , fcl.OrderedQuantity_SalesUOM / ISNULL(NULLIF(fcl.OrderedQuantity, 0), 1) AS UOMFactor
             , ISNULL(ts.Qty_IT * -1, 0)                                                AS OrderedQuantity

          FROM salesquotelinetrans_factstage                       ts
         INNER JOIN silver.cma_SalesQuoteLine_Fact fcl
            ON fcl._RecID    = ts.RecID_QL
           AND fcl._SourceID = 1;
),
salesquotelinetrans_facttransuom AS (
    SELECT t.RecID_IT                      AS RecID_IT
             , t.RecID_QL                      AS RecID_SL
             , t.OrderedQuantity * t.UOMFactor AS OrderedQuantity_SalesUOM

          FROM salesquotelinetrans_facttrans t;
)
SELECT 
           CURRENT_TIMESTAMP                                                                                    AS _CreatedDate
         , CURRENT_TIMESTAMP                                                                                    AS _ModifiedDate 
         , ROW_NUMBER() OVER (ORDER BY fcl._RecID) AS SalesQuoteLineTransKey
          ,fcl.SalesQuoteLineKey
         , ds.InventoryTransStatusKey                                                                           AS InventoryTransStatusKey
         , dt.TagKey                                                                                            AS TagKey
         , fcl.BaseUnitPrice                                                                                    AS BaseUnitPrice
         , fcl.BaseUnitPrice_TransCur                                                                           AS BaseUnitPrice_TransCur
         , fcl.DiscountAmount * ISNULL(tr.PercentOfTotal, 1)                                                    AS DiscountAmount
         , fcl.DiscountAmount_TransCur * ISNULL(tr.PercentOfTotal, 1)                                           AS DiscountAmount_TransCur
         , fcl.NetAmount * ISNULL(tr.PercentOfTotal, 1)                                                         AS NetAmount
         , fcl.NetAmount_TransCur * ISNULL(tr.PercentOfTotal, 1)                                                AS NetAmount_TransCur
         , ISNULL(tu.OrderedQuantity_SalesUOM, fcl.OrderedQuantity_SalesUOM)                                    AS OrderedQuantity_SalesUOM
         , ISNULL(tt.OrderedQuantity, fcl.OrderedQuantity)                                                      AS OrderedQuantity
         , ISNULL(tt.OrderedQuantity, 0) * fcl.OrderedQuantity_PC / ISNULL(NULLIF(fcl.OrderedQuantity, 0), 1)   AS OrderedQuantity_PC
         , ISNULL(tt.OrderedQuantity, 0) * fcl.OrderedQuantity_LB / ISNULL(NULLIF(fcl.OrderedQuantity, 0), 1)   AS OrderedQuantity_LB

         , ISNULL(tt.OrderedQuantity, 0) * fcl.OrderedQuantity_FT / ISNULL(NULLIF(fcl.OrderedQuantity, 0), 1)   AS OrderedQuantity_FT

         , ISNULL(tt.OrderedQuantity, 0) * fcl.OrderedQuantity_SQIN / ISNULL(NULLIF(fcl.OrderedQuantity, 0), 1) AS OrderedQuantity_SQIN
         , fcl.PriceUnit                                                                                        AS PriceUnit
         , fcl.RemainingQuantity                                                                                AS RemainingQuantity
         , fcl.RemainingQuantity_SalesUOM                                                                       AS RemainingQuantity_SalesUOM
         , fcl.RemainingQuantity_PC                                                                             AS RemainingQuantity_PC
         , fcl.RemainingQuantity_LB                                                                             AS RemainingQuantity_LB

         , fcl.RemainingQuantity_FT                                                                             AS RemainingQuantity_FT

         , fcl.RemainingQuantity_SQIN                                                                           AS RemainingQuantity_SQIN
         , fcl.TotalAmount * ISNULL(tr.PercentOfTotal, 1)                                                       AS TotalAmount
         , fcl.TotalAmount_TransCur * ISNULL(tr.PercentOfTotal, 1)                                              AS TotalAmount_TransCur
         , fcl.TotalUnitPrice                                                                                   AS TotalUnitPrice
         , fcl.TotalUnitPrice_TransCur                                                                          AS TotalUnitPrice_TransCur
         , ts._SourceDate                                                                                       AS _SourceDate
         , ISNULL(tt.RecID_IT, 0)                                                                               AS _RecID2
         , fcl._RecID                                                                                           AS _RecID1
         , 1                                                                                                    AS _SourceID

      FROM silver.cma_SalesQuoteLine_Fact       fcl

      LEFT JOIN salesquotelinetrans_factstage                   ts
        ON ts.RecID_QL                   = fcl._RecID
      LEFT JOIN salesquotelinetrans_facttrans                   tt
        ON tt.RecID_IT                   = ts.RecID_IT
      LEFT JOIN salesquotelinetrans_facttransuom                tu
        ON tu.RecID_IT                   = ts.RecID_IT
      LEFT JOIN salesquotelinetrans_factratio                   tr
        ON tr.RecID_IT                   = ts.RecID_IT
      LEFT JOIN salesquotelinetrans_facttag                     tg
        ON tg.RecID_IT                   = ts.RecID_IT
      LEFT JOIN silver.cma_Tag                  dt
        ON dt._RecID                     = tg.RecID_IB
       AND dt._SourceID                  = 1
      LEFT JOIN silver.cma_InventoryTransStatus ds
        ON ds.InventoryTransStatusID     = ts.STATUSISSUE
       AND ds.InventoryTransStatusTypeID = CASE WHEN ts.STATUSISSUE > 0 THEN 1 ELSE 2 END
