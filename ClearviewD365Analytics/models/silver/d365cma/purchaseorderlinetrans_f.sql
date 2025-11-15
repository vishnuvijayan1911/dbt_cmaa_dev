{{ config(materialized='table', tags=['silver'], alias='purchaseorderlinetrans_fact') }}

-- Source file: cma/cma/layers/_base/_silver/purchaseorderlinetrans_f/purchaseorderlinetrans_f.py
-- Root method: PurchaseorderlinetransFact.purchaseorderlinetrans_factdetail [PurchaseOrderLineTrans_FactDetail]
-- Inlined methods: PurchaseorderlinetransFact.purchaseorderlinetrans_factstage [PurchaseOrderLineTrans_FactStage], PurchaseorderlinetransFact.purchaseorderlinetrans_facttrans [PurchaseOrderLineTrans_FactTrans], PurchaseorderlinetransFact.purchaseorderlinetrans_facttransuom [PurchaseOrderLineTrans_FactTransUOM], PurchaseorderlinetransFact.purchaseorderlinetrans_factratio [PurchaseOrderLineTrans_FactRatio], PurchaseorderlinetransFact.purchaseorderlinetrans_factprorate [PurchaseOrderLineTrans_FactProRate], PurchaseorderlinetransFact.purchaseorderlinetrans_factadj [PurchaseOrderLineTrans_FactAdj], PurchaseorderlinetransFact.purchaseorderlinetrans_facttransadj [PurchaseOrderLineTrans_FactTransAdj], PurchaseorderlinetransFact.purchaseorderlinetrans_factprorate2 [PurchaseOrderLineTrans_FactProRate2], PurchaseorderlinetransFact.purchaseorderlinetrans_factprorate3 [PurchaseOrderLineTrans_FactProRate3], PurchaseorderlinetransFact.purchaseorderlinetrans_factprorate4 [PurchaseOrderLineTrans_FactProRate4]
-- external_table_name: PurchaseOrderLineTrans_FactDetail
-- schema_name: temp

WITH
purchaseorderlinetrans_factstage AS (
    SELECT it.recid                                                                                   AS RECID_IT
             , MAX(pl.recid)                                                                              AS RECID_PL
             , MAX(ib.recid)                                                                              AS RECID_IB
             , MAX(pl.qtyordered)                                                                          AS ORDERQTY_PL
             , MAX(it.qty)                                                                                 AS QTY_IT
             , MAX(pl.itemid)                                                                              AS ITEMID
             , MAX(it.dataareaid)                                                                         AS DATAAREAID
             , MAX(it.statusreceipt)                                                                       AS STATUSRECEIPT
             , MAX(it.statusissue)                                                                         AS STATUSISSUE
             , MAX(it.packingslipid)                                                                       AS PRODUCTRECEIPTID
             , MAX(CASE WHEN it.statusissue IN ( 1, 2 ) OR it.statusreceipt IN ( 1, 2 ) THEN 1 ELSE 0 END) AS Received
    		 , MAX(pl.modifieddatetime)																   AS _SourceDate

          FROM {{ ref('purchline') }}              pl
         INNER JOIN {{ ref('inventtransorigin') }} ito
            ON ito.dataareaid      = pl.dataareaid
           AND ito.inventtransid    = pl.inventtransid
           AND ito.itemid           = pl.itemid
         INNER JOIN {{ ref('inventtrans') }}       it
            ON it.inventtransorigin = ito.recid
         INNER JOIN {{ ref('inventdim') }}         id
            ON id.dataareaid       = it.dataareaid
           AND id.inventdimid       = it.inventdimid
          LEFT JOIN {{ ref('inventbatch') }}       ib
            ON ib.dataareaid       = id.dataareaid
           AND ib.inventbatchid     = id.inventbatchid
           AND ib.itemid            = it.itemid
         WHERE it.statusreceipt IN ( 1, 2, 3, 4, 5 )
            OR it.statusissue IN ( 1, 2, 3, 4, 5, 6 )
         GROUP BY it.recid;
),
purchaseorderlinetrans_facttrans AS (
    SELECT ts.RECID_IT                                                                                                   AS RecID_IT
             , ts.RECID_PL                                                                                                   AS RecID_PL
             , ISNULL(NULLIF(fpl.OrderedQuantity_PurchUOM, 0), 1) / ISNULL(NULLIF(fpl.OrderedQuantity, 0), 1)                AS UOMFactor
             , ISNULL(ts.QTY_IT, 0)                                                                                          AS OrderedQuantity
             , CASE WHEN dp.ItemType = 'Service' THEN 0 ELSE CASE WHEN ts.Received = 1 THEN (ts.QTY_IT) ELSE 0 END END       AS ReceivedQuantity
             , CASE WHEN dp.ItemType = 'Service' THEN 0 ELSE
             ( ts.QTY_IT) - CASE WHEN ts.Received = 1 THEN (ts.QTY_IT) ELSE 0 END END                                        AS RemainingQuantity
             , ts.PRODUCTRECEIPTID                                                                                           AS ProductReceiptID

          FROM purchaseorderlinetrans_factstage                          ts
         INNER JOIN silver.cma_PurchaseOrderLine_Fact fpl
            ON fpl._RecID    = ts.RECID_PL
           AND fpl._SourceID = 1
          LEFT JOIN silver.cma_Product                dp
            ON dp.ProductKey = fpl.ProductKey;
),
purchaseorderlinetrans_facttransuom AS (
    SELECT t.RecID_IT                                  AS RecID_IT
             , t.RecID_PL                                  AS RecID_PL
             , t.OrderedQuantity * t.UOMFactor             AS OrderedQuantity_PurchUOM
             , t.ReceivedQuantity * t.UOMFactor            AS ReceivedQuantity_PurchUOM
             , t.RemainingQuantity * t.UOMFactor           AS RemainingQuantity_PurchUOM

          FROM purchaseorderlinetrans_facttrans t;
),
purchaseorderlinetrans_factratio AS (
    SELECT ts.RECID_IT
             , CASE WHEN SUM(ts.QTY_IT) OVER (PARTITION BY ts.RECID_PL) = 0
                    THEN 1 / CAST(ISNULL(NULLIF(COUNT(1) OVER (PARTITION BY ts.RECID_PL), 0), 1) AS FLOAT)
                    ELSE
                    CAST(ts.QTY_IT AS FLOAT)
                    / CAST(ISNULL(NULLIF(SUM(ts.QTY_IT) OVER (PARTITION BY ts.RECID_PL), 0), 1) AS FLOAT)END AS PercentOfTotal

          FROM purchaseorderlinetrans_factstage ts;
),
purchaseorderlinetrans_factprorate AS (
    SELECT fsl.PurchaseOrderLineKey                                                              AS PurchaseOrderLineKey
             , ISNULL(tr.PercentOfTotal, 1)                                                          AS PercentOfTotal
             , CAST(fsl.BaseAmount * ISNULL(tr.PercentOfTotal, 1) AS NUMERIC(28, 12))                AS BaseAmount
             , CAST(fsl.BaseAmount_TransCur * ISNULL(tr.PercentOfTotal, 1) AS NUMERIC(28, 12))       AS BaseAmount_TransCur
             , CAST(fsl.OrderedPurchaseAmount * ISNULL(tr.PercentOfTotal, 1) AS MONEY)               AS OrderedPurchaseAmount
             , CAST(fsl.OrderedPurchaseAmount_TransCur * ISNULL(tr.PercentOfTotal, 1) AS MONEY)      AS OrderedPurchaseAmount_TransCur
             , CAST(fsl.IncludedCharge * ISNULL(tr.PercentOfTotal, 1) AS MONEY)                      AS IncludedCharge
             , CAST(fsl.IncludedCharge_TransCur * ISNULL(tr.PercentOfTotal, 1) AS MONEY)             AS IncludedCharge_TransCur
             , CAST(fsl.AdditionalCharge * ISNULL(tr.PercentOfTotal, 1) AS NUMERIC(28, 12))          AS AdditionalCharge
             , CAST(fsl.AdditionalCharge_TransCur * ISNULL(tr.PercentOfTotal, 1) AS NUMERIC(28, 12)) AS AdditionalCharge_TransCur
             , CAST(fsl.VendorCharge * ISNULL(tr.PercentOfTotal, 1) AS MONEY)                        AS VendorCharge
             , CAST(fsl.VendorCharge_TransCur * ISNULL(tr.PercentOfTotal, 1) AS MONEY)               AS VendorCharge_TransCur
             , CAST(fsl.NonBillableCharge * ISNULL(tr.PercentOfTotal, 1) AS MONEY)                   AS NonBillableCharge
             , CAST(fsl.NonBillableCharge_TransCur * ISNULL(tr.PercentOfTotal, 1) AS MONEY)          AS NonBillableCharge_TransCur
             , CAST(fsl.DiscountAmount * ISNULL(tr.PercentOfTotal, 1) AS NUMERIC(28, 12))            AS DiscountAmount
             , CAST(fsl.DiscountAmount_TransCur * ISNULL(tr.PercentOfTotal, 1) AS NUMERIC(28, 12))   AS DiscountAmount_TransCur
             , CAST(fsl.NetAmount * ISNULL(tr.PercentOfTotal, 1) AS MONEY)                           AS NetAmount
             , CAST(fsl.NetAmount_TransCur * ISNULL(tr.PercentOfTotal, 1) AS MONEY)                  AS NetAmount_TransCur
             , CAST(fsl.RemainingAmount * ISNULL(tr.PercentOfTotal, 1) AS MONEY)                     AS RemainingAmount
             , CAST(fsl.RemainingAmount_TransCur * ISNULL(tr.PercentOfTotal, 1) AS MONEY)            AS RemainingAmount_TransCur
             , CAST(fsl.ReceivedAmount * ISNULL(tr.PercentOfTotal, 1) AS MONEY)                      AS ReceivedAmount
             , CAST(fsl.ReceivedAmount_TransCur * ISNULL(tr.PercentOfTotal, 1) AS MONEY)             AS ReceivedAmount_TransCur
             , ISNULL(ts.RECID_IT, 0)                                                                AS RecID_IT
             , ts.RECID_PL                                                                           AS RecID_PL
             , ts.RECID_IB
             , ts.STATUSISSUE
             , ts.STATUSRECEIPT
             , CASE WHEN ROW_NUMBER() OVER (PARTITION BY fsl.PurchaseOrderLineKey
    ORDER BY ISNULL(ts.RECID_IT, 0)) = 1
                    THEN 1
                    ELSE 0 END                                                                       AS IsProrateAdj
    		, ts._SourceDate

          FROM silver.cma_PurchaseOrderLine_Fact fsl
          LEFT JOIN purchaseorderlinetrans_factstage                ts
            ON ts.RECID_PL = fsl._RecID
          LEFT JOIN purchaseorderlinetrans_factratio                tr
            ON tr.RECID_IT = ts.RECID_IT;
),
purchaseorderlinetrans_factadj AS (
    SELECT t.PurchaseOrderLineKey
             , t.RecID_IT
             , t.RecID_PL
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.BaseAmount - SUM(t.BaseAmount) OVER (PARTITION BY t.PurchaseOrderLineKey)
                         ELSE 0 END AS NUMERIC(28, 12))                                                                        AS BaseAmountAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.BaseAmount_TransCur
                              - SUM(t.BaseAmount_TransCur) OVER (PARTITION BY t.PurchaseOrderLineKey)
                         ELSE 0 END AS NUMERIC(28, 12))                                                                        AS BaseAmount_TransCurAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.DiscountAmount - SUM(t.DiscountAmount) OVER (PARTITION BY t.PurchaseOrderLineKey)
                         ELSE 0 END AS NUMERIC(28, 12))                                                                        AS DiscountAmountAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.DiscountAmount_TransCur
                              - SUM(t.DiscountAmount_TransCur) OVER (PARTITION BY t.PurchaseOrderLineKey)
                         ELSE 0 END AS NUMERIC(28, 12))                                                                        AS DiscountAmount_TransCurAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.IncludedCharge - SUM(t.IncludedCharge) OVER (PARTITION BY t.PurchaseOrderLineKey)
                         ELSE 0 END AS MONEY)                                                                                  AS IncludedChargeAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.IncludedCharge_TransCur
                              - SUM(t.IncludedCharge_TransCur) OVER (PARTITION BY t.PurchaseOrderLineKey)
                         ELSE 0 END AS MONEY)                                                                                  AS IncludedCharge_TransCurAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.AdditionalCharge - SUM(t.AdditionalCharge) OVER (PARTITION BY t.PurchaseOrderLineKey)
                         ELSE 0 END AS NUMERIC(28, 12))                                                                        AS AdditionalChargeAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.AdditionalCharge_TransCur
                              - SUM(t.AdditionalCharge_TransCur) OVER (PARTITION BY t.PurchaseOrderLineKey)
                         ELSE 0 END AS NUMERIC(28, 12))                                                                        AS AdditionalCharge_TransCurAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.VendorCharge - SUM(t.VendorCharge) OVER (PARTITION BY t.PurchaseOrderLineKey)
                         ELSE 0 END AS MONEY)                                                                                  AS VendorChargeAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.VendorCharge_TransCur
                              - SUM(t.VendorCharge_TransCur) OVER (PARTITION BY t.PurchaseOrderLineKey)
                         ELSE 0 END AS MONEY)                                                                                  AS VendorCharge_TransCurAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.NonBillableCharge - SUM(t.NonBillableCharge) OVER (PARTITION BY t.PurchaseOrderLineKey)
                         ELSE 0 END AS MONEY)                                                                                  AS NonBillableChargeAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.NonBillableCharge_TransCur
                              - SUM(t.NonBillableCharge_TransCur) OVER (PARTITION BY t.PurchaseOrderLineKey)
                         ELSE 0 END AS MONEY)                                                                                  AS NonBillableCharge_TransCurAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.RemainingAmount - SUM(t.RemainingAmount) OVER (PARTITION BY t.PurchaseOrderLineKey)
                         ELSE 0 END AS MONEY)                                                                                  AS RemainingAmountAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.RemainingAmount_TransCur
                              - SUM(t.RemainingAmount_TransCur) OVER (PARTITION BY t.PurchaseOrderLineKey)
                         ELSE 0 END AS MONEY)                                                                                  AS RemainingAmount_TransCurAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.NetAmount - SUM(t.NetAmount) OVER (PARTITION BY t.PurchaseOrderLineKey) ELSE 0 END AS MONEY) AS NetAmountAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.NetAmount_TransCur - SUM(t.NetAmount_TransCur) OVER (PARTITION BY t.PurchaseOrderLineKey)
                         ELSE 0 END AS MONEY)                                                                                  AS NetAmount_TransCurAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.OrderedPurchaseAmount
                              - SUM(t.OrderedPurchaseAmount) OVER (PARTITION BY t.PurchaseOrderLineKey)
                         ELSE 0 END AS MONEY)                                                                                  AS OrderedPurchaseAmountAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.OrderedPurchaseAmount_TransCur
                              - SUM(t.OrderedPurchaseAmount_TransCur) OVER (PARTITION BY t.PurchaseOrderLineKey)
                         ELSE 0 END AS MONEY)                                                                                  AS OrderedPurchaseAmount_TransCurAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.ReceivedAmount - SUM(t.ReceivedAmount) OVER (PARTITION BY t.PurchaseOrderLineKey)
                         ELSE 0 END AS MONEY)                                                                                  AS ReceivedAmountAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.ReceivedAmount_TransCur
                              - SUM(t.ReceivedAmount_TransCur) OVER (PARTITION BY t.PurchaseOrderLineKey)
                         ELSE 0 END AS MONEY)                                                                                  AS ReceivedAmount_TransCurAdj

          FROM purchaseorderlinetrans_factprorate                        t
         INNER JOIN silver.cma_PurchaseOrderLine_Fact fcl
            ON fcl.PurchaseOrderLineKey = t.PurchaseOrderLineKey
),
purchaseorderlinetrans_facttransadj AS (
    SELECT PurchaseOrderLineKey
             , RecID_IT
             , RecID_PL
             , BaseAmountAdj
             , BaseAmount_TransCurAdj
             , DiscountAmountAdj
             , DiscountAmount_TransCurAdj
             , IncludedChargeAdj
             , IncludedCharge_TransCurAdj
             , AdditionalChargeAdj
             , AdditionalCharge_TransCurAdj
             , VendorChargeAdj
             , VendorCharge_TransCurAdj
             , NonBillableChargeAdj
             , NonBillableCharge_TransCurAdj
             , RemainingAmountAdj
             , RemainingAmount_TransCurAdj
             , NetAmountAdj
             , NetAmount_TransCurAdj
             , OrderedPurchaseAmountAdj
             , OrderedPurchaseAmount_TransCurAdj
             , ReceivedAmountAdj
             , ReceivedAmount_TransCurAdj

          FROM purchaseorderlinetrans_factadj
         WHERE BaseAmountAdj                     <> 0
            OR BaseAmount_TransCurAdj            <> 0
            OR DiscountAmountAdj                 <> 0
            OR DiscountAmount_TransCurAdj        <> 0
            OR IncludedChargeAdj                 <> 0
            OR IncludedCharge_TransCurAdj        <> 0
            OR AdditionalChargeAdj               <> 0
            OR AdditionalCharge_TransCurAdj      <> 0
            OR VendorChargeAdj                   <> 0
            OR VendorCharge_TransCurAdj          <> 0
            OR NonBillableChargeAdj              <> 0
            OR NonBillableCharge_TransCurAdj     <> 0
            OR RemainingAmountAdj                <> 0
            OR RemainingAmount_TransCurAdj       <> 0
            OR NetAmountAdj                      <> 0
            OR NetAmount_TransCurAdj             <> 0
            OR OrderedPurchaseAmountAdj          <> 0
            OR OrderedPurchaseAmount_TransCurAdj <> 0
            OR ReceivedAmountAdj                 <> 0
            OR ReceivedAmount_TransCurAdj        <> 0;
),
purchaseorderlinetrans_factprorate2 AS (
    Select 
        t.PurchaseOrderLineKey,
        t.PercentOfTotal,
        t.BaseAmount + ta.BaseAmountAdj AS                                                      BaseAmount,
        t.BaseAmount_TransCur + ta.BaseAmount_TransCurAdj AS                                    BaseAmount_TransCur,
        t.OrderedPurchaseAmount + ta.OrderedPurchaseAmountAdj AS                                OrderedPurchaseAmount,
        t.OrderedPurchaseAmount_TransCur + ta.OrderedPurchaseAmount_TransCurAdj AS              OrderedPurchaseAmount_TransCur,
        t.IncludedCharge + ta.IncludedChargeAdj AS                                              IncludedCharge,
        t.IncludedCharge_TransCur + ta.IncludedCharge_TransCurAdj AS                            IncludedCharge_TransCur,
         t.AdditionalCharge + ta.AdditionalChargeAdj AS                                          AdditionalCharge,
        t.AdditionalCharge_TransCur + ta.AdditionalCharge_TransCurAdj AS                        AdditionalCharge_TransCur,
        t.VendorCharge + ta.VendorChargeAdj AS                                                  VendorCharge,
        t.VendorCharge_TransCur + ta.VendorCharge_TransCurAdj AS                                VendorCharge_TransCur,
        t.NonBillableCharge + ta.NonBillableChargeAdj AS                                        NonBillableCharge,
        t.NonBillableCharge_TransCur + ta.NonBillableCharge_TransCurAdj AS                      NonBillableCharge_TransCur,
        t.DiscountAmount + ta.DiscountAmountAdj AS                                              DiscountAmount,
        t.DiscountAmount_TransCur + ta.DiscountAmount_TransCurAdj AS                            DiscountAmount_TransCur,
        t.NetAmount + ta.NetAmountAdj AS                                                        NetAmount,
        t.NetAmount_TransCur + ta.NetAmount_TransCurAdj AS                                      NetAmount_TransCur,
        t.RemainingAmount + ta.RemainingAmountAdj AS                                            RemainingAmount,
        t.RemainingAmount_TransCur + ta.RemainingAmount_TransCurAdj AS                          RemainingAmount_TransCur,
        t.ReceivedAmount + ta.ReceivedAmountAdj AS                                              ReceivedAmount,
        t.ReceivedAmount_TransCur + ta.ReceivedAmount_TransCurAdj AS                            ReceivedAmount_TransCur,
        t.RecID_IT,
        t.RecID_PL,
        t.RECID_IB,
        t.STATUSISSUE,
        t.STATUSRECEIPT,
        t.IsProrateAdj,
        t._SourceDate

        FROM purchaseorderlinetrans_factprorate t
        INNER JOIN purchaseorderlinetrans_facttransadj ta
            ON ta.PurchaseOrderLineKey = t.PurchaseOrderLineKey
           AND ta.RecID_IT             = t.RecID_IT;
),
purchaseorderlinetrans_factprorate3 AS (
    Select 
        t.PurchaseOrderLineKey,
        t.PercentOfTotal,
        t.BaseAmount,
        t.BaseAmount_TransCur,
        t.OrderedPurchaseAmount,
        t.OrderedPurchaseAmount_TransCur,
        t.IncludedCharge,
        t.IncludedCharge_TransCur,
         t.AdditionalCharge,
        t.AdditionalCharge_TransCur,
        t.VendorCharge,
        t.VendorCharge_TransCur,
        t.NonBillableCharge,
        t.NonBillableCharge_TransCur,
        t.DiscountAmount,
        t.DiscountAmount_TransCur,
        t.NetAmount,
        t.NetAmount_TransCur,
        t.RemainingAmount,
        t.RemainingAmount_TransCur,
        t.ReceivedAmount,
        t.ReceivedAmount_TransCur,
        t.RecID_IT,
        t.RecID_PL,
        t.RECID_IB,
        t.STATUSISSUE,
        t.STATUSRECEIPT,
        t.IsProrateAdj,
        t._SourceDate

        FROM purchaseorderlinetrans_factprorate t
        LEFT JOIN purchaseorderlinetrans_facttransadj ta
            ON ta.PurchaseOrderLineKey = t.PurchaseOrderLineKey
           AND ta.RecID_IT             = t.RecID_IT
           WHERE  ta.PurchaseOrderLineKey IS NULL
),
purchaseorderlinetrans_factprorate4 AS (
    Select * from   purchaseorderlinetrans_factprorate2
      UNION ALL
      Select * from   purchaseorderlinetrans_factprorate3
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY fpl._RecID) AS PurchaseOrderLineTransKey,
    fpl.PurchaseOrderLineKey                                                                                    AS PurchaseOrderLineKey
         , ds.InventoryTransStatusKey                                                                                  AS InventoryTransStatusKey
         , dt.TagKey                                                                                                   AS TagKey
         , it.costamountposted                                                                                         AS PostedCost
         , it.costamountadjustment                                                                                     AS PostedCostAdjustment
         , it.costamountphysical                                                                                       AS PhysicalCost
         , (it.costamountposted + it.costamountadjustment)                                                             AS FinancialCost
         , ISNULL(ts.AdditionalCharge, fpl.AdditionalCharge)                                                           AS AdditionalCharge
         , ISNULL(ts.AdditionalCharge_TransCur, fpl.AdditionalCharge_TransCur)                                         AS AdditionalCharge_TransCur
         , ISNULL(ts.BaseAmount, fpl.BaseAmount)                                                                       AS BaseAmount
         , ISNULL(ts.BaseAmount_TransCur, fpl.BaseAmount_TransCur)                                                     AS BaseAmount_TransCur
         , fpl.BaseUnitPrice                                                                                           AS BaseUnitPrice
         , fpl.BaseUnitPrice_TransCur                                                                                  AS BaseUnitPrice_TransCur
         , ISNULL(ts.DiscountAmount, fpl.DiscountAmount)                                                               AS DiscountAmount
         , ISNULL(ts.DiscountAmount_TransCur, fpl.DiscountAmount_TransCur)                                             AS DiscountAmount_TransCur
         , ISNULL(ts.PercentOfTotal, 1)                                                                                AS PercentOfTotal
         , ISNULL(ts.IncludedCharge, fpl.IncludedCharge)                                                               AS IncludedCharge
         , ISNULL(ts.IncludedCharge_TransCur, fpl.IncludedCharge_TransCur)                                             AS IncludedCharge_TransCur
         , ISNULL(ts.NetAmount, fpl.NetAmount)                                                                         AS NetAmount
         , ISNULL(ts.NetAmount_TransCur, fpl.NetAmount_TransCur)                                                       AS NetAmount_TransCur
         , ISNULL(ts.NonBillableCharge, fpl.NonBillableCharge)                                                         AS NonBillableCharge
         , ISNULL(ts.NonBillableCharge_TransCur, fpl.NonBillableCharge_TransCur)                                       AS NonBillableCharge_TransCur
         , ISNULL(tt.OrderedQuantity, 1) * fpl.OrderedQuantity_PurchUOM / ISNULL(NULLIF(fpl.OrderedQuantity, 0), 1)    AS OrderedQuantity_PurchUOM
         , ISNULL(tt.OrderedQuantity, fpl.OrderedQuantity)                                                             AS OrderedQuantity
         , ISNULL(tt.OrderedQuantity, 1) * fpl.OrderedQuantity_LB / ISNULL(NULLIF(fpl.OrderedQuantity, 0), 1)          AS OrderedQuantity_LB
         , ISNULL(tt.OrderedQuantity, 1) * fpl.OrderedQuantity_PC / ISNULL(NULLIF(fpl.OrderedQuantity, 0), 1)          AS OrderedQuantity_PC

		 , ISNULL(tt.OrderedQuantity, 1) * fpl.OrderedQuantity_FT / ISNULL(NULLIF(fpl.OrderedQuantity, 0), 1)          AS OrderedQuantity_FT

         , ISNULL(tt.OrderedQuantity, 1) * fpl.OrderedQuantity_SQIN / ISNULL(NULLIF(fpl.OrderedQuantity, 0), 1)        AS OrderedQuantity_SQIN
         , fpl.PriceUnit                                                                                               AS PriceUnit
         , ISNULL(ts.ReceivedAmount, fpl.ReceivedAmount)                                                               AS ReceivedAmount
         , ISNULL(ts.ReceivedAmount_TransCur, fpl.ReceivedAmount_TransCur)                                             AS ReceivedAmount_TransCur
         , ISNULL(tt.ReceivedQuantity, 1) * fpl.ReceivedQuantity_PurchUOM / ISNULL(NULLIF(fpl.ReceivedQuantity, 0), 1) AS ReceivedQuantity_PurchUOM
         , ISNULL(tt.ReceivedQuantity, fpl.ReceivedQuantity)                                                           AS ReceivedQuantity
         , ISNULL(tt.ReceivedQuantity, 1) * fpl.ReceivedQuantity_LB / ISNULL(NULLIF(fpl.ReceivedQuantity, 0), 1)       AS ReceivedQuantity_LB
         , ISNULL(tt.ReceivedQuantity, 1) * fpl.ReceivedQuantity_PC / ISNULL(NULLIF(fpl.ReceivedQuantity, 0), 1)       AS ReceivedQuantity_PC

		 , ISNULL(tt.ReceivedQuantity, 1) * fpl.ReceivedQuantity_FT / ISNULL(NULLIF(fpl.ReceivedQuantity, 0), 1)       AS ReceivedQuantity_FT

         , ISNULL(tt.ReceivedQuantity, 1) * fpl.ReceivedQuantity_SQIN / ISNULL(NULLIF(fpl.ReceivedQuantity, 0), 1)      AS ReceivedQuantity_SQIN
        , ISNULL(tu.RemainingQuantity_PurchUOM, fpl.RemainingQuantity_PurchUOM) * ISNULL(NULLIF(fpl.totalunitprice, 0), fpl.BaseUnitPrice)
                     / ISNULL(NULLIF(fpl.priceunit, 0), 1)                                                              AS RemainingAmount
         , ISNULL(tu.RemainingQuantity_PurchUOM, fpl.RemainingQuantity_PurchUOM) * ISNULL(NULLIF(fpl.totalunitprice_transcur, 0), fpl.BaseUnitPrice_TransCur)
                     / ISNULL(NULLIF(fpl.priceunit, 0), 1)                                          AS RemainingAmount_TransCur
         , ISNULL(tu.RemainingQuantity_PurchUOM, fpl.RemainingQuantity_PurchUOM)                                       AS RemainingQuantity_PurchUOM
         , ISNULL(tt.RemainingQuantity, fpl.RemainingQuantity)                                                         AS RemainingQuantity
         , ISNULL(tt.RemainingQuantity, 1) * fpl.RemainingQuantity_LB / ISNULL(NULLIF(fpl.RemainingQuantity, 0), 1)    AS RemainingQuantity_LB
         , ISNULL(tt.RemainingQuantity, 1) * fpl.RemainingQuantity_PC / ISNULL(NULLIF(fpl.RemainingQuantity, 0), 1)    AS RemainingQuantity_PC

		 , ISNULL(tt.RemainingQuantity, 1) * fpl.RemainingQuantity_FT / ISNULL(NULLIF(fpl.RemainingQuantity, 0), 1)    AS RemainingQuantity_FT

         , ISNULL(tt.RemainingQuantity, 1) * fpl.RemainingQuantity_SQIN / ISNULL(NULLIF(fpl.RemainingQuantity, 0), 1)   AS RemainingQuantity_SQIN
         , ISNULL(ts.OrderedPurchaseAmount, fpl.OrderedPurchaseAmount)                                                 AS OrderedPurchaseAmount
         , ISNULL(ts.OrderedPurchaseAmount_TransCur, fpl.OrderedPurchaseAmount_TransCur)                               AS OrderedPurchaseAmount_TransCur
         , fpl.TotalUnitPrice                                                                                          AS TotalUnitPrice
         , fpl.TotalUnitPrice_TransCur                                                                                 AS TotalUnitPrice_TransCur
         , ISNULL(ts.VendorCharge, fpl.VendorCharge)                                                                   AS VendorCharge
         , ISNULL(ts.VendorCharge_TransCur, fpl.VendorCharge_TransCur)                                                 AS VendorCharge_TransCur
         , tt.ProductReceiptID                                                                                         AS ProductReceiptID
		 , ts._SourceDate
         , ISNULL(ts.RecID_IT, 0)                                                                                      AS _RecID2
         , fpl._RecID                                                                                                  AS _RecID1
         , 1                                                                                                           AS _SourceID
         ,  CURRENT_TIMESTAMP  AS  _CreatedDate
         , CURRENT_TIMESTAMP AS _ModifiedDate 

      FROM silver.cma_PurchaseOrderLine_Fact    fpl
       LEFT JOIN purchaseorderlinetrans_factprorate4                 ts
        ON ts.PurchaseOrderLineKey       = fpl.PurchaseOrderLineKey
      LEFT JOIN purchaseorderlinetrans_facttrans                   tt
        ON tt.RecID_IT                   = ts.RecID_IT
      LEFT JOIN purchaseorderlinetrans_factratio                   tr
        ON tr.RecID_IT                   = tt.RecID_IT
      LEFT JOIN silver.cma_PurchaseOrderLine    dpl
        ON dpl._RecID                    = tt.RecID_PL
       AND dpl._SourceID                 = 1
      LEFT JOIN purchaseorderlinetrans_facttransuom                tu
        ON tu.RecID_IT                   = tt.RecID_IT
      LEFT JOIN {{ ref('inventtrans') }}          it
        ON it.recid                     = tt.RecID_IT
      LEFT JOIN silver.cma_Tag                  dt
        ON dt._RecID                     = ts.RECID_IB
       AND dt._SourceID                  = 1
      LEFT JOIN silver.cma_InventoryTransStatus ds
        ON ds.InventoryTransStatusID     = CASE WHEN ts.STATUSRECEIPT > 0 THEN ts.STATUSRECEIPT ELSE ts.STATUSISSUE END
       AND ds.InventoryTransStatusTypeID = CASE WHEN ts.STATUSRECEIPT > 0 THEN 2 ELSE 1 END
