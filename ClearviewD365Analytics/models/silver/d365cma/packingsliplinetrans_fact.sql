{{ config(materialized='table', tags=['silver']) }}

-- Source file: cma/cma/layers/_base/_silver/packingsliplinetrans_fact/packingsliplinetrans_fact.py
-- Root method: PackingsliplinetransFact.get_detail_table_query [PackingSlipLineTrans_FactDetail]
-- Inlined methods: PackingsliplinetransFact.get_parent_trans_query [PackingSlipLineTrans_FactParentTrans], PackingsliplinetransFact.get_parent_item_query [PackingSlipLineTrans_FactParentItem], PackingsliplinetransFact.get_master_item_query [PackingSlipLineTrans_FactMasterItem], PackingsliplinetransFact.get_invent_trans_query [PackingSlipLineTrans_FactInventTrans], PackingsliplinetransFact.get_stage_query [PackingSlipLineTrans_FactStage], PackingsliplinetransFact.get_master_tag_query [PackingSlipLineTrans_FactMasterTag], PackingsliplinetransFact.get_find_ratio_query [PackingSlipLineTrans_FactRatio], PackingsliplinetransFact.get_trans_query [PackingSlipLineTrans_FactTrans], PackingsliplinetransFact.get_adj_query [PackingSlipLineTrans_FactAdj], PackingsliplinetransFact.get_trans_adj_query [PackingSlipLineTrans_FactTransAdj], PackingsliplinetransFact.get_update_trans_adjustment_query [PackingSlipLineTrans_FactTrans_updated]
-- external_table_name: PackingSlipLineTrans_FactDetail
-- schema_name: temp

WITH
packingsliplinetrans_factparenttrans AS (
    SELECT DISTINCT
          ib.recid               AS RecID_IB
        , it.dataareaid          AS DATAAREAID
        , ib.cmartsparent         AS ParentTag
        , ib.cmamasterinventbatch AS MasterTag
        , MAX(ib1.itemid)         AS MasterITEMID
        , MAX(ib2.itemid)         AS ParentITEMID
      FROM {{ ref('custpackingsliptrans') }} cpst
    INNER JOIN {{ ref('inventtrans') }}     it
        ON it.dataareaid    = cpst.dataareaid
      AND it.packingslipid  = cpst.packingslipid
      AND it.itemid         = cpst.itemid
    INNER JOIN {{ ref('inventdim') }}       id
        ON id.dataareaid    = it.dataareaid
      AND id.inventdimid    = it.inventdimid
    INNER JOIN  {{ ref('inventbatch') }}      ib
        ON ib.dataareaid    = id.dataareaid
      AND ib.inventbatchid  = id.inventbatchid
      LEFT JOIN  {{ ref('inventbatch') }}      ib1
        ON ib1.dataareaid   = ib.dataareaid
      AND ib1.inventbatchid = ib.cmamasterinventbatch
      LEFT JOIN  {{ ref('inventbatch') }}      ib2
        ON ib2.dataareaid   = ib.dataareaid
      AND ib2.inventbatchid = ib.cmartsparent
    WHERE ib.cmartsparent <> ''
    GROUP BY ib.recid
            , it.dataareaid
            , ib.cmartsparent
            , ib.cmamasterinventbatch
),
packingsliplinetrans_factparentitem AS (
    SELECT t.*
          FROM (   SELECT pt.RecID_IB
                        , it.itemid        AS ParentItemID
                        , id.inventcolorid AS ParentProductLength
                        , id.inventstyleid AS ParentProductColor
                        , id.inventsizeid  AS ParentProductWidth
                        , id.configid      AS ParentProductConfig
                        , ROW_NUMBER() OVER (PARTITION BY pt.RecID_IB
    ORDER BY pt.RecID_IB)                  AS RankVal
                    FROM packingsliplinetrans_factparenttrans         pt
                    INNER JOIN {{ ref('inventtrans') }} it
                      ON pt.DATAAREAID   = it.dataareaid
                      AND pt.ParentITEMID  = it.itemid
                    INNER JOIN {{ ref('inventdim') }}   id
                      ON id.dataareaid   = it.dataareaid
                      AND id.inventdimid   = it.inventdimid
                      AND id.inventbatchid = pt.ParentTag) t
        WHERE t.RankVal = 1
),
packingsliplinetrans_factmasteritem AS (
    SELECT t.*
          FROM (   SELECT pt.RecID_IB
                        , it.itemid        AS MasterItemID
                        , id.inventcolorid AS MasterProductLength
                        , id.inventstyleid AS MasterProductColor
                        , id.inventsizeid  AS MasterProductWidth
                        , id.configid      AS MasterProductConfig
                        , ROW_NUMBER() OVER (PARTITION BY pt.RecID_IB
    ORDER BY pt.RecID_IB)                  AS RankVal
                    FROM packingsliplinetrans_factparenttrans         pt
                    INNER JOIN {{ ref('inventtrans') }} it
                      ON pt.DATAAREAID   = it.dataareaid
                      AND pt.MasterITEMID  = it.itemid
                    INNER JOIN {{ ref('inventdim') }}   id
                      ON id.dataareaid   = it.dataareaid
                      AND id.inventdimid   = it.inventdimid
                      AND id.inventbatchid = pt.MasterTag) t
        WHERE t.RankVal = 1
),
packingsliplinetrans_factinventtrans AS (
    SELECT it.recid                                                                                   AS RecID_IT
            , MAX(it.qty)                                                                                 AS Qty_IT
            , MAX(it.statusissue)                                                                         AS STATUSISSUE
            , MAX(it.statusreceipt)                                                                       AS STATUSRECEIPT
            , MAX(it.packingslipid)                                                                       AS PACKINGSLIPID
            , MAX(ib.cmartsparent)                                                                        AS ParentTag
            , MAX(ito.itemid)                                                                             AS ITEMID
            , MAX(ito.dataareaid)                                                                        AS DATAAREAID
            , MAX(ito.inventtransid)                                                                      AS INVENTTRANSID
            , MAX(it.inventdimid)                                                                         AS INVENTDIMID
            , MAX(CASE WHEN it.statusissue IN ( 1, 2 ) OR it.statusreceipt IN ( 1, 2 ) THEN 1 ELSE 0 END) AS Shipped
            , MAX(CASE WHEN it.statusissue IN ( 2 ) OR it.statusreceipt IN ( 2 ) THEN 1 ELSE 0 END)       AS ShippedNotInvoiced
            , MAX(cpst.recid)                                                                            AS RecID_CPST
            , MAX(cpst.inventqty)                                                                         AS InventQty_CPST
            , COALESCE(MAX(tmi.MasterItemID), '')                                                                       AS MasterItemID
            , COALESCE(MAX(tmi.MasterProductLength),'')                                                                AS MasterProductLength
            , COALESCE((MAX(tmi.MasterProductColor)), '')                                                                 AS MasterProductColor
            , COALESCE(MAX(tmi.MasterProductWidth) ,'')                                                                AS MasterProductWidth
            , COALESCE(MAX(tmi.MasterProductConfig), '')                                                                AS MasterProductConfig
            , COALESCE(MAX(tpi.ParentItemID), '')                                                                       AS ParentItemID
            , COALESCE(MAX(tpi.ParentProductLength), '')                                                               AS ParentProductLength
            , COALESCE(MAX(tpi.ParentProductColor), '')                                                                AS ParentProductColor
            , COALESCE(MAX(tpi.ParentProductWidth), '')                                                                 AS ParentProductWidth
            , COALESCE(MAX(tpi.ParentProductConfig), '')                                                                AS ParentProductConfig
          FROM {{ ref('custpackingsliptrans') }}   cpst
        INNER JOIN {{ ref('inventtrans') }}       it
            ON it.dataareaid   = cpst.dataareaid
          AND it.packingslipid = cpst.packingslipid
          AND it.itemid        = cpst.itemid
        INNER JOIN {{ ref('inventtransorigin') }} ito
            ON ito.recid       = it.inventtransorigin
            AND ito.inventtransid = cpst.inventtransid
          AND (it.statusissue IN ( 1, 2, 3, 4, 5, 6 ) OR it.statusreceipt IN ( 1, 2, 3, 4, 5 ))
        INNER JOIN {{ ref('inventdim') }}         id
            ON id.dataareaid   = it.dataareaid
          AND id.inventdimid   = it.inventdimid
          LEFT JOIN {{ ref('inventbatch') }}       ib
            ON ib.dataareaid   = it.dataareaid
          AND ib.inventbatchid = id.inventbatchid
          AND ib.inventbatchid <> ''
          AND ib.itemid        = it.itemid
          LEFT JOIN packingsliplinetrans_factparentitem           tpi
            ON tpi.RecID_IB     = ib.recid
          LEFT JOIN packingsliplinetrans_factmasteritem           tmi
            ON tmi.RecID_IB     = ib.recid
        GROUP BY it.recid;
),
packingsliplinetrans_factstage AS (
    SELECT cpst.dataareaid                                                         AS LegalEntityID
        , tps.STATUSISSUE                                                          AS STATUSISSUE
        , tps.STATUSRECEIPT                                                        AS STATUSRECEIPT
        , tps.MasterItemID                                                         AS MasterItemID
        , tps.MasterProductLength                                                  AS MasterProductLength
        , tps.MasterProductColor                                                   AS MasterProductColor
        , tps.MasterProductWidth                                                   AS MasterProductWidth
        , tps.MasterProductConfig                                                  AS MasterProductConfig
        , tps.ParentItemID                                                         AS ParentItemID
        , tps.ParentProductColor                                                   AS ParentProductColor
        , tps.ParentProductConfig                                                  AS ParentProductConfig
        , tps.ParentProductLength                                                  AS ParentProductLength
        , tps.ParentProductWidth                                                   AS ParentProductWidth
        , tps.ParentTag                                                            AS ParentTagID
        , id.inventbatchid                                                         AS TagID
        , cpst.itemid                                                              AS ItemID
        , (CASE WHEN tps.Shipped = 1 THEN tps.Qty_IT ELSE 0 END * cpst.qty / ISNULL(NULLIF(cpst.inventqty, 0), 1))
          * -1                                                                     AS ShippedQuantity_SalesUOM
        , (CASE WHEN tps.Shipped = 1 THEN tps.Qty_IT ELSE 0 END) * -1              AS ShippedQuantity
        , (CASE WHEN tps.ShippedNotInvoiced = 1 THEN tps.Qty_IT ELSE 0 END * cpst.qty
            / ISNULL(NULLIF(cpst.inventqty, 0), 1)) * -1                            AS ShippedNotInvoicedQuantity_SalesUOM
        , (CASE WHEN tps.ShippedNotInvoiced = 1 THEN tps.Qty_IT ELSE 0 END) * -1   AS ShippedNotInvoicedQuantity
        , tps.ShippedNotInvoiced                                                   AS ShippedNotInvoiced
        , (tps.Qty_IT - CASE WHEN tps.Shipped = 1 THEN tps.Qty_IT ELSE 0 END * cpst.qty
            / ISNULL(NULLIF(cpst.inventqty, 0), 1)) * -1                            AS RemainingQuantity_SalesUOM
        , (tps.Qty_IT - CASE WHEN tps.Shipped = 1 THEN tps.Qty_IT ELSE 0 END) * -1 AS RemainingQuantity
        , tps.RecID_IT                                                             AS RecID_IT
        , cpst.recid                                                              AS RecID_CPST
        , sl.recid                                                                AS RecID_SL 
      FROM packingsliplinetrans_factinventtrans                  tps
    INNER JOIN {{ ref('custpackingsliptrans') }} cpst
        ON cpst.recid      = tps.RecID_CPST
    INNER JOIN {{ ref('salesline') }}            sl
        ON sl.dataareaid   = cpst.dataareaid
      AND sl.inventtransid = cpst.inventtransid
      AND sl.itemid        = cpst.itemid
      LEFT JOIN {{ ref('inventdim') }}            id
        ON id.dataareaid   = tps.DATAAREAID
      AND id.inventdimid   = tps.INVENTDIMID;
),
packingsliplinetrans_factmastertag AS (
    SELECT dt.LegalEntityID
        , dt.TagID
        , dt.ItemID
        , MAX(dt1.TagKey) AS MasterTagKey
      FROM packingsliplinetrans_factstage       t1
      LEFT JOIN silver.cma_Tag dt
        ON dt.LegalEntityID  = t1.LegalEntityID
      AND dt.TagID          = t1.TagID
      AND dt.ItemID         = t1.ItemID
      LEFT JOIN silver.cma_Tag dt1
        ON dt1.LegalEntityID = dt.LegalEntityID
      AND dt1.TagID         = dt.MasterTagID
    GROUP BY dt.LegalEntityID
            , dt.TagID
            , dt.ItemID;
),
packingsliplinetrans_factratio AS (
    SELECT ts.RecID_IT                                                                                          AS RecID_IT
        , CASE WHEN SUM(-1 * ts.Qty_IT) OVER (PARTITION BY ts.RecID_CPST) = 0
                THEN 1 / CAST(ISNULL(NULLIF(COUNT(1) OVER (PARTITION BY ts.RecID_CPST), 0), 1) AS FLOAT)
                ELSE
                CAST(ts.Qty_IT AS FLOAT) * -1
                / CAST(ISNULL(NULLIF(SUM(-1 * ts.Qty_IT) OVER (PARTITION BY ts.RecID_CPST), 0), 1) AS FLOAT)END AS PercentOfTotal

      FROM packingsliplinetrans_factinventtrans ts;
),
packingsliplinetrans_facttrans AS (
    SELECT 
              ROW_NUMBER() OVER (ORDER BY frl.PackingSlipLineKey) AS [Index]
            , frl.PackingSlipLineKey                                                                AS PackingSlipLineKey
            , ts.ShippedQuantity_SalesUOM
            , ts.ShippedQuantity
            , ts.ShippedNotInvoicedQuantity_SalesUOM
            , ts.ShippedNotInvoicedQuantity
            , ts.LegalEntityID
            , ts.STATUSISSUE
            , ts.STATUSRECEIPT
            , ts.MasterItemID
            , ts.MasterProductLength
            , ts.MasterProductColor
            , ts.MasterProductWidth
            , ts.MasterProductConfig
            , ts.ParentItemID
            , ts.ParentProductColor
            , ts.ParentProductConfig
            , ts.ParentProductLength
            , ts.ParentProductWidth
            , ts.ParentTagID
            , ts.TagID
            , ts.ItemID
            , CAST(frl.OrderedQuantity * ISNULL(tr.PercentOfTotal, 1) AS NUMERIC(20, 6))            AS OrderedQuantity
            , CAST(frl.OrderedQuantity_SalesUOM * ISNULL(tr.PercentOfTotal, 1) AS NUMERIC(20, 6))   AS OrderedQuantity_SalesUOM
            , CAST(frl.OrderedQuantity_LB * ISNULL(tr.PercentOfTotal, 1) AS NUMERIC(20, 6))         AS OrderedQuantity_LB
            , CAST(frl.OrderedQuantity_PC * ISNULL(tr.PercentOfTotal, 1) AS NUMERIC(20, 6))         AS OrderedQuantity_PC
            -- , CAST(frl.OrderedQuantity_TON * ISNULL(tr.PercentOfTotal, 1) AS NUMERIC(20, 6))        AS OrderedQuantity_TON
            , CAST(frl.OrderedQuantity_FT * ISNULL(tr.PercentOfTotal, 1) AS NUMERIC(20, 6))         AS OrderedQuantity_FT
            -- , CAST(frl.OrderedQuantity_IN * ISNULL(tr.PercentOfTotal, 1) AS NUMERIC(20, 6))         AS OrderedQuantity_IN
            , CAST(frl.OrderedQuantity_SQIN * ISNULL(tr.PercentOfTotal, 1) AS NUMERIC(20, 6))       AS OrderedQuantity_SQIN
            , CAST(frl.ShippedAmount * ISNULL(tr.PercentOfTotal, 1) AS NUMERIC(28, 12))             AS ShippedAmount
            , CAST(frl.ShippedAmount_TransCur * ISNULL(tr.PercentOfTotal, 1) AS MONEY)              AS ShippedAmount_TransCur
            , CAST(frl.ShippedNotInvoicedAmount * ISNULL(tr.PercentOfTotal, 1) AS NUMERIC(28, 12))  AS ShippedNotInvoicedAmount
            , CAST(frl.ShippedNotInvoicedAmount_TransCur * ISNULL(tr.PercentOfTotal, 1) AS MONEY)   AS ShippedNotInvoicedAmount_TransCur
            , ts.ShippedNotInvoiced                                                                 AS ShippedNotInvoiceTransCount
            , CAST(frl.RemainingQuantity * ISNULL(tr.PercentOfTotal, 1) AS NUMERIC(20, 6))          AS RemainingQuantity
            , CAST(frl.RemainingQuantity_SalesUOM * ISNULL(tr.PercentOfTotal, 1) AS NUMERIC(20, 6)) AS RemainingQuantity_SalesUOM
            , CAST(frl.RemainingQuantity_LB * ISNULL(tr.PercentOfTotal, 1) AS NUMERIC(20, 6))       AS RemainingQuantity_LB
            , CAST(frl.RemainingQuantity_PC * ISNULL(tr.PercentOfTotal, 1) AS NUMERIC(20, 6))       AS RemainingQuantity_PC
            --, CAST(frl.RemainingQuantity_TON * ISNULL(tr.PercentOfTotal, 1) AS NUMERIC(20, 6))      AS RemainingQuantity_TON
            , CAST(frl.RemainingQuantity_FT * ISNULL(tr.PercentOfTotal, 1) AS NUMERIC(20, 6))       AS RemainingQuantity_FT
            -- , CAST(frl.RemainingQuantity_IN * ISNULL(tr.PercentOfTotal, 1) AS NUMERIC(20, 6))       AS RemainingQuantity_IN
            , CAST(frl.RemainingQuantity_SQIN * ISNULL(tr.PercentOfTotal, 1) AS NUMERIC(20, 6))     AS RemainingQuantity_SQIN
            , ISNULL(ts.RecID_IT, 0)                                                                AS _RecID2
            , ts.RecID_CPST
            , ts.RecID_SL
            , CASE WHEN ROW_NUMBER() OVER (PARTITION BY frl.PackingSlipLineKey
    ORDER BY ISNULL(ts.RecID_IT, 0)) = 1
                    THEN 1
                    ELSE 0 END                                                                       AS IsProrateAdj
          FROM silver.cma_PackingSlipLine_Fact frl
        INNER JOIN packingsliplinetrans_factstage              ts
            ON ts.RecID_CPST = frl._RecID
          AND frl._SourceID = 1
        INNER JOIN packingsliplinetrans_factratio              tr
            ON tr.RecID_IT   = ts.RecID_IT;
),
packingsliplinetrans_factadj AS (
    SELECT t.PackingSlipLineKey
        , t._RecID2
        , t.RecID_CPST
        , CAST(CASE WHEN t.IsProrateAdj = 1
                    THEN fcl.OrderedQuantity - SUM(CAST(t.OrderedQuantity AS NUMERIC(20, 6))) OVER (PARTITION BY t.PackingSlipLineKey)
                    ELSE 0 END AS NUMERIC(20, 6))  AS OrderedQuantityAdj
        , CAST(CASE WHEN t.IsProrateAdj = 1
                    THEN fcl.OrderedQuantity_SalesUOM
                          - SUM(CAST(t.OrderedQuantity_SalesUOM AS NUMERIC(20, 6) )) OVER (PARTITION BY t.PackingSlipLineKey)
                    ELSE 0 END AS NUMERIC(20, 6))  AS OrderedQuantity_SalesUOMAdj
        , CAST(CASE WHEN t.IsProrateAdj = 1
                    THEN fcl.OrderedQuantity_LB - SUM(CAST(t.OrderedQuantity_LB AS NUMERIC(20, 6))) OVER (PARTITION BY t.PackingSlipLineKey)
                    ELSE 0 END AS NUMERIC(20, 6))  AS OrderedQuantity_LBAdj
        , CAST(CASE WHEN t.IsProrateAdj = 1
                    THEN fcl.OrderedQuantity_PC - SUM(CAST(t.OrderedQuantity_PC AS NUMERIC(20, 6))) OVER (PARTITION BY t.PackingSlipLineKey)
                    ELSE 0 END AS NUMERIC(20, 6))  AS OrderedQuantity_PCAdj
        -- , CAST(CASE WHEN t.IsProrateAdj = 1
        --             THEN fcl.OrderedQuantity_TON - SUM(CAST(t.OrderedQuantity_TON AS NUMERIC(20, 6)) ) OVER (PARTITION BY t.PackingSlipLineKey)
        --             ELSE 0 END AS NUMERIC(20, 6))  AS OrderedQuantity_TONAdj
        , CAST(CASE WHEN t.IsProrateAdj = 1
                    THEN fcl.OrderedQuantity_FT - SUM(CAST(t.OrderedQuantity_FT AS NUMERIC(20, 6))) OVER (PARTITION BY t.PackingSlipLineKey)
                    ELSE 0 END AS NUMERIC(20, 6))  AS OrderedQuantity_FTAdj
      --  , CAST(CASE WHEN t.IsProrateAdj = 1
      --              THEN fcl.OrderedQuantity_IN - SUM(CAST(t.OrderedQuantity_IN AS NUMERIC(20, 6))) OVER (PARTITION BY t.PackingSlipLineKey)
      --              ELSE 0 END AS NUMERIC(20, 6))  AS OrderedQuantity_INAdj
        , CAST(CASE WHEN t.IsProrateAdj = 1
                    THEN fcl.OrderedQuantity_SQIN
                          - SUM(CAST(t.OrderedQuantity_SQIN AS NUMERIC(20, 6))) OVER (PARTITION BY t.PackingSlipLineKey)
                    ELSE 0 END AS NUMERIC(20, 6))  AS OrderedQuantity_SQINAdj
        , CAST(CASE WHEN t.IsProrateAdj = 1
                    THEN fcl.ShippedAmount - SUM(CAST(t.ShippedAmount AS NUMERIC(28, 12))) OVER (PARTITION BY t.PackingSlipLineKey)
                    ELSE 0 END AS NUMERIC(28, 12)) AS ShippedAmountAdj
        , CAST(CASE WHEN t.IsProrateAdj = 1
                    THEN fcl.ShippedAmount_TransCur
                          - SUM(CAST(t.ShippedAmount_TransCur AS NUMERIC(20, 6))) OVER (PARTITION BY t.PackingSlipLineKey)
                    ELSE 0 END AS MONEY)           AS ShippedAmount_TransCurAdj
        , CAST(CASE WHEN t.IsProrateAdj = 1
                    THEN fcl.ShippedNotInvoicedAmount
                          - SUM(CAST(t.ShippedNotInvoicedAmount AS NUMERIC(28, 12))) OVER (PARTITION BY t.PackingSlipLineKey)
                    ELSE 0 END AS NUMERIC(28, 12)) AS ShippedNotInvoicedAmountAdj
        , CAST(CASE WHEN t.IsProrateAdj = 1
                    THEN fcl.ShippedNotInvoicedAmount_TransCur
                          - SUM(CAST(t.ShippedNotInvoicedAmount_TransCur AS NUMERIC(20, 6))) OVER (PARTITION BY t.PackingSlipLineKey)
                    ELSE 0 END AS MONEY)           AS ShippedNotInvoicedAmount_TransCurAdj
        , CAST(CASE WHEN t.IsProrateAdj = 1
                    THEN fcl.RemainingQuantity - SUM(CAST(t.RemainingQuantity AS NUMERIC(20, 6))) OVER (PARTITION BY t.PackingSlipLineKey)
                    ELSE 0 END AS NUMERIC(20, 6))  AS RemainingQuantityAdj
        , CAST(CASE WHEN t.IsProrateAdj = 1
                    THEN fcl.RemainingQuantity_SalesUOM
                          - SUM(CAST(t.RemainingQuantity_SalesUOM AS NUMERIC(20, 6))) OVER (PARTITION BY t.PackingSlipLineKey)
                    ELSE 0 END AS NUMERIC(20, 6))  AS RemainingQuantity_SalesUOMAdj
        , CAST(CASE WHEN t.IsProrateAdj = 1
                    THEN fcl.RemainingQuantity_LB
                          - SUM(CAST(t.RemainingQuantity_LB AS NUMERIC(20, 6))) OVER (PARTITION BY t.PackingSlipLineKey)
                    ELSE 0 END AS NUMERIC(20, 6))  AS RemainingQuantity_LBAdj
        , CAST(CASE WHEN t.IsProrateAdj = 1
                    THEN fcl.RemainingQuantity_PC
                          - SUM(CAST(t.RemainingQuantity_PC AS NUMERIC(20, 6))) OVER (PARTITION BY t.PackingSlipLineKey)
                    ELSE 0 END AS NUMERIC(20, 6))  AS RemainingQuantity_PCAdj
        -- , CAST(CASE WHEN t.IsProrateAdj = 1
        --            THEN fcl.RemainingQuantity_TON
        --                 - SUM(CAST(t.RemainingQuantity_TON AS NUMERIC(20, 6))) OVER (PARTITION BY t.PackingSlipLineKey)
        --            ELSE 0 END AS NUMERIC(20, 6))  AS RemainingQuantity_TONAdj
        , CAST(CASE WHEN t.IsProrateAdj = 1
                    THEN fcl.RemainingQuantity_FT
                          - SUM(CAST(t.RemainingQuantity_FT AS NUMERIC(20, 6))) OVER (PARTITION BY t.PackingSlipLineKey)
                    ELSE 0 END AS NUMERIC(20, 6))  AS RemainingQuantity_FTAdj
        -- , CAST(CASE WHEN t.IsProrateAdj = 1
        --             THEN fcl.RemainingQuantity_IN
        --                  - SUM(CAST(t.RemainingQuantity_IN AS NUMERIC(20, 6))) OVER (PARTITION BY t.PackingSlipLineKey)
        --             ELSE 0 END AS NUMERIC(20, 6))  AS RemainingQuantity_INAdj
        , CAST(CASE WHEN t.IsProrateAdj = 1
                    THEN fcl.RemainingQuantity_SQIN
                          - SUM(CAST(t.RemainingQuantity_SQIN AS NUMERIC(20, 6))) OVER (PARTITION BY t.PackingSlipLineKey)
                    ELSE 0 END AS NUMERIC(20, 6))  AS RemainingQuantity_SQINAdj
      FROM packingsliplinetrans_facttrans                        t
    INNER JOIN silver.cma_PackingSlipLine_Fact fcl
        ON fcl.PackingSlipLineKey = t.PackingSlipLineKey
),
packingsliplinetrans_facttransadj AS (
    SELECT PackingSlipLineKey
        , _RecID2
        , RecID_CPST
        , OrderedQuantityAdj
        , OrderedQuantity_SalesUOMAdj
        , OrderedQuantity_LBAdj
        , OrderedQuantity_PCAdj
        -- , OrderedQuantity_TONAdj
        , OrderedQuantity_FTAdj
        -- , OrderedQuantity_INAdj
        , OrderedQuantity_SQINAdj
        , ShippedAmountAdj
        , ShippedAmount_TransCurAdj
        , ShippedNotInvoicedAmountAdj
        , ShippedNotInvoicedAmount_TransCurAdj
        , RemainingQuantityAdj
        , RemainingQuantity_SalesUOMAdj
        , RemainingQuantity_LBAdj
        , RemainingQuantity_PCAdj
        -- , RemainingQuantity_TONAdj
        , RemainingQuantity_FTAdj
        -- , RemainingQuantity_INAdj
        , RemainingQuantity_SQINAdj
      FROM packingsliplinetrans_factadj
    WHERE OrderedQuantityAdj                   <> 0
        OR OrderedQuantity_SalesUOMAdj          <> 0
        OR OrderedQuantity_LBAdj                <> 0
        OR OrderedQuantity_PCAdj                <> 0
      -- OR OrderedQuantity_TONAdj               <> 0
        OR OrderedQuantity_FTAdj                <> 0
      -- OR OrderedQuantity_INAdj                <> 0
        OR OrderedQuantity_SQINAdj              <> 0
        OR ShippedAmountAdj                     <> 0
        OR ShippedAmount_TransCurAdj            <> 0
        OR ShippedNotInvoicedAmountAdj          <> 0
        OR ShippedNotInvoicedAmount_TransCurAdj <> 0
        OR RemainingQuantityAdj                 <> 0
        OR RemainingQuantity_SalesUOMAdj        <> 0
        OR RemainingQuantity_LBAdj              <> 0
        OR RemainingQuantity_PCAdj              <> 0
      -- OR RemainingQuantity_TONAdj             <> 0
        OR RemainingQuantity_FTAdj              <> 0
      -- OR RemainingQuantity_INAdj              <> 0
        OR RemainingQuantity_SQINAdj            <> 0;
),
packingsliplinetrans_facttrans_updated AS (
    SELECT DISTINCT
      t.[Index] AS [Index],
           CAST(t.OrderedQuantity + ta.OrderedQuantityAdj AS NUMERIC(32,16)) AS OrderedQuantity
        ,  CAST(t.OrderedQuantity_SalesUOM + ta.OrderedQuantity_SalesUOMAdj AS NUMERIC(32,16)) AS OrderedQuantity_SalesUOM
        ,  CAST(t.OrderedQuantity_LB + ta.OrderedQuantity_LBAdj AS NUMERIC(32,16)) AS OrderedQuantity_LB
        ,  CAST(t.OrderedQuantity_PC + ta.OrderedQuantity_PCAdj AS NUMERIC(32,16)) AS OrderedQuantity_PC
        ,  CAST(t.OrderedQuantity_FT + ta.OrderedQuantity_FTAdj AS NUMERIC(32,16)) AS OrderedQuantity_FT
        ,  CAST(t.OrderedQuantity_SQIN + ta.OrderedQuantity_SQINAdj AS NUMERIC(32,16)) AS  OrderedQuantity_SQIN
        ,  CAST(t.ShippedAmount + ta.ShippedAmountAdj AS NUMERIC(32,16)) AS ShippedAmount
        ,  CAST(t.ShippedAmount_TransCur + ta.ShippedAmount_TransCurAdj AS NUMERIC(32,16)) AS ShippedAmount_TransCur
        ,  CAST(t.ShippedNotInvoicedAmount + ta.ShippedNotInvoicedAmountAdj AS NUMERIC(32,16)) AS ShippedNotInvoicedAmount
        ,  CAST(t.ShippedNotInvoicedAmount_TransCur + ta.ShippedNotInvoicedAmount_TransCurAdj AS NUMERIC(32,16)) AS ShippedNotInvoicedAmount_TransCur
        ,  CAST(t.RemainingQuantity + ta.RemainingQuantityAdj AS NUMERIC(32,16)) AS RemainingQuantity
        ,  CAST(t.RemainingQuantity_SalesUOM + ta.RemainingQuantity_SalesUOMAdj AS NUMERIC(32,16)) AS RemainingQuantity_SalesUOM
        ,  CAST(t.RemainingQuantity_LB + ta.RemainingQuantity_LBAdj AS NUMERIC(32,16)) AS RemainingQuantity_LB
        ,  CAST(t.RemainingQuantity_PC + ta.RemainingQuantity_PCAdj AS NUMERIC(32,16)) AS RemainingQuantity_PC
        ,  CAST(t.RemainingQuantity_FT + ta.RemainingQuantity_FTAdj AS NUMERIC(32,16)) AS RemainingQuantity_FT
        ,  CAST(t.RemainingQuantity_SQIN + ta.RemainingQuantity_SQINAdj AS NUMERIC(32,16)) AS RemainingQuantity_SQIN
      FROM packingsliplinetrans_facttrans         t
    INNER JOIN packingsliplinetrans_facttransadj ta
        ON ta.PackingSlipLineKey = t.PackingSlipLineKey
      AND ta._RecID2            = t._RecID2
)
SELECT DISTINCT 
      ROW_NUMBER() OVER (ORDER BY frl.PackingSlipLineKey) AS PackingSlipLineTransKey
      , frl.PackingSlipLineKey                                                                               AS PackingSlipLineKey
    , dis.InventoryTransStatusKey                                                                          AS InventoryTransStatusKey
    , ISNULL(dp1.ProductKey, -1)                                                                           AS MasterProductKey
    , ISNULL(dp.ProductKey, frl.ProductKey)                                                                AS ParentProductKey
    , tmt.MasterTagKey                                                                                     AS MasterTagKey
    , ISNULL(dt1.TagKey, dt.TagKey)                                                                        AS ParentTagKey
    , solt.SalesOrderLineTransKey                                                                          AS SalesOrderLineTransKey
    , dt.TagKey                                                                                            AS TagKey
    , ISNULL(tt.OrderedQuantity, frl.OrderedQuantity)                                                      AS OrderedQuantity
    , ISNULL(tt.OrderedQuantity_SalesUOM, frl.OrderedQuantity_SalesUOM)                                    AS OrderedQuantity_SalesUOM
    , ISNULL(tt.OrderedQuantity_LB, frl.OrderedQuantity_LB)                                                AS OrderedQuantity_LB
    , ISNULL(tt.OrderedQuantity_PC, frl.OrderedQuantity_PC)                                                AS OrderedQuantity_PC
    -- , ISNULL(tt.OrderedQuantity_TON, frl.OrderedQuantity_TON)                                              AS OrderedQuantity_TON
    , ISNULL(tt.OrderedQuantity_FT, frl.OrderedQuantity_FT)                                                AS OrderedQuantity_FT
    -- , ISNULL(tt.OrderedQuantity_IN, frl.OrderedQuantity_IN)                                                AS OrderedQuantity_IN
    , ISNULL(tt.OrderedQuantity_SQIN, frl.OrderedQuantity_SQIN)                                            AS OrderedQuantity_SQIN
    , ISNULL(tt.ShippedAmount, frl.ShippedAmount)                                                          AS ShippedAmount
    , ISNULL(tt.ShippedAmount_TransCur, frl.ShippedAmount_TransCur)                                        AS ShippedAmount_TransCur
    , ISNULL(tt.ShippedNotInvoicedAmount, frl.ShippedNotInvoicedAmount)                                    AS ShippedNotInvoicedAmount
    , ISNULL(tt.ShippedNotInvoicedAmount_TransCur, frl.ShippedNotInvoicedAmount_TransCur)                  AS ShippedNotInvoicedAmount_TransCur
    , ISNULL(tt.ShippedQuantity, frl.ShippedQuantity)                                                      AS ShippedQuantity
    , ISNULL(tt.ShippedQuantity_SalesUOM, 1) * frl.ShippedQuantity_SalesUOM
      / ISNULL(NULLIF(frl.ShippedQuantity_SalesUOM, 0), 1)                                                 AS ShippedQuantity_SalesUOM
    , ISNULL(tt.ShippedQuantity, 1) * frl.ShippedQuantity_LB / ISNULL(NULLIF(frl.ShippedQuantity, 0), 1)   AS ShippedQuantity_LB
    , ISNULL(tt.ShippedQuantity, 1) * frl.ShippedQuantity_PC / ISNULL(NULLIF(frl.ShippedQuantity, 0), 1)   AS ShippedQuantity_PC
    -- , ISNULL(tt.ShippedQuantity, 1) * frl.ShippedQuantity_TON / ISNULL(NULLIF(frl.ShippedQuantity, 0), 1)  AS ShippedQuantity_TON
    , ISNULL(tt.ShippedQuantity, 1) * frl.ShippedQuantity_FT / ISNULL(NULLIF(frl.ShippedQuantity, 0), 1)   AS ShippedQuantity_FT
    -- , ISNULL(tt.ShippedQuantity, 1) * frl.ShippedQuantity_IN / ISNULL(NULLIF(frl.ShippedQuantity, 0), 1)   AS ShippedQuantity_IN
    , ISNULL(tt.ShippedQuantity, 1) * frl.ShippedQuantity_SQIN / ISNULL(NULLIF(frl.ShippedQuantity, 0), 1) AS ShippedQuantity_SQIN
    , ISNULL(tt.ShippedNotInvoicedQuantity, frl.ShippedNotInvoicedQuantity)                                AS ShippedNotInvoicedQuantity
    , ISNULL(tt.ShippedNotInvoicedQuantity_SalesUOM, 1) * frl.ShippedNotInvoicedQuantity_SalesUOM
      / ISNULL(NULLIF(frl.ShippedNotInvoicedQuantity_SalesUOM, 0), 1)                                      AS ShippedNotInvoicedQuantity_SalesUOM
    , ISNULL(tt.ShippedNotInvoicedQuantity, 1) * frl.ShippedNotInvoicedQuantity_LB
      / ISNULL(NULLIF(frl.ShippedNotInvoicedQuantity, 0), 1)                                               AS ShippedNotInvoicedQuantity_LB
    , ISNULL(tt.ShippedNotInvoicedQuantity, 1) * frl.ShippedNotInvoicedQuantity_PC
      / ISNULL(NULLIF(frl.ShippedNotInvoicedQuantity, 0), 1)                                               AS ShippedNotInvoicedQuantity_PC
    -- , ISNULL(tt.ShippedNotInvoicedQuantity, 1) * frl.ShippedNotInvoicedQuantity_TON
    --   / ISNULL(NULLIF(frl.ShippedNotInvoicedQuantity, 0), 1)                                               AS ShippedNotInvoicedQuantity_TON
    , ISNULL(tt.ShippedNotInvoicedQuantity, 1) * frl.ShippedNotInvoicedQuantity_FT
      / ISNULL(NULLIF(frl.ShippedNotInvoicedQuantity, 0), 1)                                               AS ShippedNotInvoicedQuantity_FT
    -- , ISNULL(tt.ShippedNotInvoicedQuantity, 1) * frl.ShippedNotInvoicedQuantity_In
    --   / ISNULL(NULLIF(frl.ShippedNotInvoicedQuantity, 0), 1)                                               AS ShippedNotInvoicedQuantity_IN
    , ISNULL(tt.ShippedNotInvoicedQuantity, 1) * frl.ShippedNotInvoicedQuantity_SqIn
      / ISNULL(NULLIF(frl.ShippedNotInvoicedQuantity, 0), 1)                                               AS ShippedNotInvoicedQuantity_SQIN
    , tt.ShippedNotInvoiceTransCount
    , CASE WHEN tt.ShippedNotInvoiceTransCount = 1
            THEN DATEDIFF(
                    d
                  , NULLIF(CASE WHEN dis.InventoryTransStatusID IN ( 1, 2 ) THEN it.datephysical ELSE NULL END, '1/1/1900')
                  , GETDATE())
            ELSE NULL END                                                                                   AS ShippedNotInvoicedDays
    , ISNULL(tt.RemainingQuantity, frl.RemainingQuantity)                                                  AS RemainingQuantity
    , ISNULL(tt.RemainingQuantity_SalesUOM, frl.RemainingQuantity_SalesUOM)                                AS RemainingQuantity_SalesUOM
    , ISNULL(tt.RemainingQuantity_LB, frl.RemainingQuantity_LB)                                            AS RemainingQuantity_LB
    , ISNULL(tt.RemainingQuantity_PC, frl.RemainingQuantity_PC)                                            AS RemainingQuantity_PC
    -- , ISNULL(tt.RemainingQuantity_TON, frl.RemainingQuantity_TON)                                          AS RemainingQuantity_TON
    -- , ISNULL(tt.RemainingQuantity_IN, frl.RemainingQuantity_IN)                                            AS RemainingQuantity_IN
    , ISNULL(tt.RemainingQuantity_FT, frl.RemainingQuantity_FT)                                            AS RemainingQuantity_FT
    , ISNULL(tt.RemainingQuantity_SQIN, frl.RemainingQuantity_SQIN)                                        AS RemainingQuantity_SQIN
    , 1                                                                                                    AS _SourceID
    , ISNULL(tt._RecID2, 0)                                                                                AS _RecID2
    , frl._RecID                                                                                           AS _RECID
  FROM silver.cma_PackingSlipLine_Fact          frl
  LEFT JOIN packingsliplinetrans_facttrans_updated                       tt
    ON frl.PackingSlipLineKey         = tt.PackingSlipLineKey
  LEFT JOIN silver.cma_InventoryTransStatus     dis
    ON dis.InventoryTransStatusID     = CASE WHEN tt.STATUSISSUE > 0 THEN tt.STATUSISSUE ELSE tt.STATUSRECEIPT END
  AND dis.InventoryTransStatusTypeID = CASE WHEN tt.STATUSISSUE > 0 THEN 1 ELSE 2 END
  LEFT JOIN silver.cma_Tag                      dt
    ON dt.LegalEntityID               = tt.LegalEntityID
  AND dt.TagID                       = tt.TagID
  AND dt.ItemID                      = tt.ItemID
  LEFT JOIN silver.cma_Tag                      dt1
    ON dt1.LegalEntityID              = tt.LegalEntityID
  AND dt1.TagID                      = tt.ParentTagID
  AND dt1.ItemID                     = tt.ParentItemID
  LEFT JOIN {{ ref('inventtrans') }}              it
    ON it.recid                      = tt._RecID2
  LEFT JOIN packingsliplinetrans_factmastertag                   tmt
    ON tmt.LegalEntityID              = dt.LegalEntityID
  AND tmt.TagID                      = dt.TagID
  AND tmt.ItemID                     = dt.ItemID
  LEFT JOIN silver.cma_Product                  dp
    ON dp.LegalEntityID               = tt.LegalEntityID
  AND dp.ItemID                      = tt.ParentItemID
  AND dp.ProductLength               = tt.ParentProductLength
  AND dp.ProductColor                = tt.ParentProductColor
  AND dp.ProductWidth                = tt.ParentProductWidth
  AND dp.ProductConfig               = tt.ParentProductConfig
  LEFT JOIN silver.cma_Product                  dp1
    ON dp1.LegalEntityID              = tt.LegalEntityID
  AND dp1.ItemID                     = tt.MasterItemID
  AND dp1.ProductLength              = tt.MasterProductLength
  AND dp1.ProductColor               = tt.MasterProductColor
  AND dp1.ProductWidth               = tt.MasterProductWidth
  AND dp1.ProductConfig              = tt.MasterProductConfig
  LEFT JOIN silver.cma_SalesOrderLineTrans_Fact solt
    ON solt._RecID2                   = tt._RecID2
  AND solt._SourceID                 = 1;
