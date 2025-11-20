{{ config(materialized='table', tags=['silver'], alias='salesorderlinetrans_fact') }}

-- Source file: cma/cma/layers/_base/_silver/salesorderlinetrans_f/salesorderlinetrans_f.py
-- Root method: SalesorderlinetransFact.get_detail_query [SalesOrderLineTrans_FactDetail]
-- Inlined methods: SalesorderlinetransFact.get_reserved_aging_query [SalesOrderLineTrans_FactReservedAging], SalesorderlinetransFact.get_reserved_query [SalesOrderLineTrans_FactReserved], SalesorderlinetransFact.get_uom_conversion_query [SalesOrderLineTrans_FactUOMConversion], SalesorderlinetransFact.get_stage_query [SalesOrderLineTrans_FactStage], SalesorderlinetransFact.get_trans_query [SalesOrderLineTrans_FactTrans], SalesorderlinetransFact.get_trans_uom_query [SalesOrderLineTrans_FactTransUOM], SalesorderlinetransFact.get_ratio_query [SalesOrderLineTrans_FactRatio], SalesorderlinetransFact.get_pro_rate_query [SalesOrderLineTrans_FactProRate], SalesorderlinetransFact.get_adj_query [SalesOrderLineTrans_FactAdj], SalesorderlinetransFact.get_trans_adj_query [SalesOrderLineTrans_FactTransAdj], SalesorderlinetransFact.get_pro_rate2_query [SalesOrderLineTrans_FactProRate2], SalesorderlinetransFact.get_pro_rate3_query [SalesOrderLineTrans_FactProRate3], SalesorderlinetransFact.get_pro_rate4_query [SalesOrderLineTrans_FactProRate4], SalesorderlinetransFact.get_detail1_query [SalesOrderLineTrans_FactDetail1]
-- external_table_name: SalesOrderLineTrans_FactDetail
-- schema_name: temp

WITH
salesorderlinetrans_factreservedaging AS (
    SELECT ISNULL(NULLIF(CAST(sl.shippingdateconfirmed AS DATE), '1900-01-01'), sl.shippingdaterequested) AS ReservedDate
        , it.recid                                                                                            AS RecID_IT
      FROM {{ ref('salesline') }}              sl
    INNER JOIN {{ ref('inventtransorigin') }} ito
        ON ito.dataareaid      = sl.dataareaid
      AND ito.inventtransid    = sl.inventtransid
      AND ito.itemid           = sl.itemid
    INNER JOIN {{ ref('inventtrans') }}       it
        ON it.inventtransorigin = ito.recid
    WHERE it.statusissue IN ( 4, 5 );
),
salesorderlinetrans_factreserved AS (
    SELECT it.qty * -1 AS PhysicalReservedQuantity
        , sl.recid   AS RecID_SL
        , it.recid   AS RecID_IT
      FROM {{ ref('inventtransoriginsalesline') }} osl
    INNER JOIN {{ ref('salesline') }}             sl
        ON sl.dataareaid       = osl.saleslinedataareaid
      AND sl.inventtransid     = osl.saleslineinventtransid
    INNER JOIN {{ ref('inventtrans') }}           it
        ON it.inventtransorigin = osl.inventtransorigin
    WHERE it.statusreceipt = 0
      AND it.statusissue   = 4;
),
salesorderlinetrans_factuomconversion AS (
    SELECT tr.PhysicalReservedQuantity									  AS PhysicalReservedQuantity
          , tr.PhysicalReservedQuantity * ISNULL(vuc2.factor, 1) * 0.0005           AS PhysicalReservedQuantity_SalesUOM
          , tr.PhysicalReservedQuantity * ISNULL(vuc.factor, 1)            AS PhysicalReservedQuantity_FT
          , tr.PhysicalReservedQuantity * ISNULL(vuc2.factor, 1)           AS PhysicalReservedQuantity_LB
          , ROUND(tr.PhysicalReservedQuantity * ISNULL(vuc5.factor, 1), 0) AS PhysicalReservedQuantity_PC
          , tr.RecID_IT													  AS _RecID
        FROM salesorderlinetrans_factreserved                    tr
      INNER JOIN {{ ref('salesorderline_f') }} sol
          ON sol._RecID          = tr.RecID_SL
        AND sol._SourceID       = 1
      INNER JOIN {{ ref('product_d') }}             dp
          ON dp.ProductKey       = sol.ProductKey
        LEFT JOIN {{ ref('vwuomconversion_ft') }}     vuc
          ON vuc.legalentitykey  = sol.LegalEntityKey
        AND vuc.productkey      = dp.ProductKey
        AND vuc.fromuom         = dp.InventoryUOM
    --  AND vuc.touom           = 'FT'
        LEFT JOIN {{ ref('vwuomconversion_lb') }}     vuc2
          ON vuc2.legalentitykey = sol.LegalEntityKey
        AND vuc2.productkey     = dp.ProductKey
        AND vuc2.fromuom        = dp.InventoryUOM
    --  AND vuc2.touom          = 'LB'
        LEFT JOIN {{ ref('vwuomconversion_pc') }}     vuc5
          ON vuc5.legalentitykey = sol.LegalEntityKey
        AND vuc5.productkey     = dp.ProductKey
        AND vuc5.fromuom        = dp.InventoryUOM
    --  AND vuc5.touom          = 'PC'
),
salesorderlinetrans_factstage AS (
    SELECT it.recid                                                                                      AS RecID_IT
        , MAX(sl.recid)                                                                                      AS RecID_SL
        , MAX(ib.recid)                                                                                      AS RecID_IB
        , MAX(it.dataareaid)                                                                                 AS DATAAREAID
        , MAX(sl.itemid)                                                                                     AS ItemID
        , MAX(sl.currencycode)                                                                               AS CurrencyCode
        , MAX(sl.qtyordered)                                                                                 AS QtyOrdered_SL
        , MAX(it.qty)                                                                                        AS Qty_IT
        , MAX(it.statusissue)                                                                                AS STATUSISSUE
        , MAX(it.statusreceipt)                                                                              AS STATUSRECEIPT
        , MAX(it.packingslipid)                                                                              AS PackingSlipID
        , MAX(it.inventdimid)                                                                                AS InventDimID
        , MAX(CASE WHEN it.statusissue IN ( 1, 2 ) OR it.statusreceipt IN ( 1, 2 ) THEN 1 ELSE 0 END)        AS Shipped
        , MAX(CASE WHEN it.statusissue IN ( 4 ) THEN CAST(it.modifieddatetime AS DATE)ELSE '1900-01-01' END) AS AvailableDate
        , MAX(sl.modifieddatetime)                                                                           AS _SourceDate
    FROM {{ ref('salesline') }}              sl
    INNER JOIN {{ ref('salestable') }}        st
        ON st.dataareaid       = sl.dataareaid
    AND st.salesid           = sl.salesid
    INNER JOIN {{ ref('inventtransorigin') }} ito
        ON ito.dataareaid      = sl.dataareaid
    AND ito.inventtransid    = sl.inventtransid
    AND ito.itemid           = sl.itemid
    INNER JOIN {{ ref('inventtrans') }}       it
        ON it.inventtransorigin = ito.recid
    INNER JOIN {{ ref('inventdim') }}         id
        ON id.dataareaid       = it.dataareaid
    AND id.inventdimid       = it.inventdimid
    LEFT JOIN  {{ ref('inventbatch') }}      ib
        ON ib.dataareaid       = id.dataareaid
    AND ib.inventbatchid     = id.inventbatchid
    AND ib.itemid            = sl.itemid
    WHERE (it.statusissue IN ( 1, 2, 3, 4, 5, 6 ) OR it.statusreceipt IN ( 1, 2, 3, 4, 5 ))
    GROUP BY it.recid;
),
salesorderlinetrans_facttrans AS (
    SELECT ts.RecID_IT                                                                                                  AS RecID_IT
        , ts.RecID_SL                                                                                                       AS RecID_SL
        , fsl.OrderedQuantity_SalesUOM / ISNULL(NULLIF(fsl.OrderedQuantity, 0), 1)                                          AS UOMFactor
        , ISNULL((ts.Qty_IT * -1), 0)                                                                                       AS OrderedQuantity
        , CASE WHEN dp.ItemType = 'Service' THEN 0 ELSE CASE WHEN ts.Shipped = 1 THEN (ts.Qty_IT * -1) ELSE 0 END END       AS ShippedQuantity
        , CASE WHEN dp.ItemType = 'Service'
                THEN 0
                ELSE (ts.Qty_IT * -1) - CASE WHEN ts.Shipped = 1 THEN (ts.Qty_IT * -1) ELSE 0 END END                        AS RemainingQuantity
      FROM salesorderlinetrans_factstage                       ts
    INNER JOIN {{ ref('salesorderline_f') }} fsl
        ON fsl._RecID    = ts.RecID_SL
      AND fsl._SourceID = 1
      LEFT JOIN {{ ref('product_d') }}             dp
        ON dp.ProductKey = fsl.ProductKey;
),
salesorderlinetrans_facttransuom AS (
    SELECT t.RecID_IT                            AS RecID_IT
      , t.RecID_SL                                 AS RecID_SL
      , t.OrderedQuantity * t.UOMFactor            AS OrderedQuantity_SalesUOM
      , t.ShippedQuantity * t.UOMFactor            AS ShippedQuantity_SalesUOM
      , t.RemainingQuantity * t.UOMFactor          AS RemainingQuantity_SalesUOM
    FROM salesorderlinetrans_facttrans t;
),
salesorderlinetrans_factratio AS (
    SELECT ts.RecID_IT                                                                                        AS RecID_IT
      , CASE WHEN SUM(-1 * ts.Qty_IT) OVER (PARTITION BY ts.RecID_SL) = 0
              THEN 1 / CAST(ISNULL(NULLIF(COUNT(1) OVER (PARTITION BY ts.RecID_SL), 0), 1) AS FLOAT)
              ELSE
              CAST(ts.Qty_IT AS FLOAT) * -1
              / CAST(ISNULL(NULLIF(SUM(-1 * ts.Qty_IT) OVER (PARTITION BY ts.RecID_SL), 0), 1) AS FLOAT)END     AS PercentOfTotal
    FROM salesorderlinetrans_factstage ts;
),
salesorderlinetrans_factprorate AS (
    SELECT fsl.SalesOrderLineKey                                                     AS SalesOrderLineKey
            , ISNULL(tr.PercentOfTotal, 1)                                                   AS PercentOfTotal
            , CAST(fsl.BaseAmount * ISNULL(tr.PercentOfTotal, 1) AS MONEY)                   AS BaseAmount
            , CAST(fsl.BaseAmount_TransCur * ISNULL(tr.PercentOfTotal, 1) AS MONEY)          AS BaseAmount_TransCur
            , CAST(fsl.OrderedSalesAmount * ISNULL(tr.PercentOfTotal, 1) AS NUMERIC(28, 12)) AS OrderedSalesAmount
            , CAST(fsl.OrderedSalesAmount_TransCur * ISNULL(tr.PercentOfTotal, 1) AS MONEY)  AS OrderedSalesAmount_TransCur
            , CAST(fsl.IncludedCharge * ISNULL(tr.PercentOfTotal, 1) AS MONEY)               AS IncludedCharge
            , CAST(fsl.IncludedCharge_TransCur * ISNULL(tr.PercentOfTotal, 1) AS MONEY)      AS IncludedCharge_TransCur
            , CAST(fsl.AdditionalCharge * ISNULL(tr.PercentOfTotal, 1) AS MONEY)             AS AdditionalCharge
            , CAST(fsl.AdditionalCharge_TransCur * ISNULL(tr.PercentOfTotal, 1) AS MONEY)    AS AdditionalCharge_TransCur
            , CAST(fsl.CustomerCharge * ISNULL(tr.PercentOfTotal, 1) AS MONEY)               AS CustomerCharge
            , CAST(fsl.CustomerCharge_TransCur * ISNULL(tr.PercentOfTotal, 1) AS MONEY)      AS CustomerCharge_TransCur
            , CAST(fsl.NonBillableCharge * ISNULL(tr.PercentOfTotal, 1) AS MONEY)            AS NonBillableCharge
            , CAST(fsl.NonBillableCharge_TransCur * ISNULL(tr.PercentOfTotal, 1) AS MONEY)   AS NonBillableCharge_TransCur
            , CAST(fsl.DiscountAmount * ISNULL(tr.PercentOfTotal, 1) AS MONEY)               AS DiscountAmount
            , CAST(fsl.DiscountAmount_TransCur * ISNULL(tr.PercentOfTotal, 1) AS MONEY)      AS DiscountAmount_TransCur
            , CAST(fsl.NetAmount * ISNULL(tr.PercentOfTotal, 1) AS MONEY)                    AS NetAmount
            , CAST(fsl.NetAmount_TransCur * ISNULL(tr.PercentOfTotal, 1) AS MONEY)           AS NetAmount_TransCur
            , CAST(fsl.RemainingAmount * ISNULL(tr.PercentOfTotal, 1) AS MONEY)              AS RemainingAmount
            , CAST(fsl.RemainingAmount_TransCur * ISNULL(tr.PercentOfTotal, 1) AS MONEY)     AS RemainingAmount_TransCur
            , ts.PackingSlipID                                                               AS PackingSlipID
            , ts.AvailableDate                                                               AS AvailableDate
            , ISNULL(ts.RecID_IT, 0)                                                         AS RecID_IT
            , ts.RecID_SL                                                                    AS RecID_SL
            , ts.RecID_IB
            , ts.STATUSISSUE
            , ts.STATUSRECEIPT
            , CASE WHEN ROW_NUMBER() OVER (PARTITION BY fsl.SalesOrderLineKey
    ORDER BY ISNULL(ts.RecID_IT, 0)) = 1
                    THEN 1
                    ELSE 0 END                                                                AS IsProrateAdj
            , ts._SourceDate
          FROM {{ ref('salesorderline_f') }} fsl
          LEFT JOIN salesorderlinetrans_factstage             ts
            ON ts.RecID_SL = fsl._RecID
          LEFT JOIN salesorderlinetrans_factratio             tr
            ON tr.RecID_IT = ts.RecID_IT;
),
salesorderlinetrans_factadj AS (
    SELECT t.SalesOrderLineKey
        , t.RecID_IT
        , t.RecID_SL
        , CAST(CASE WHEN t.IsProrateAdj = 1
                    THEN fcl.BaseAmount - SUM(t.BaseAmount) OVER (PARTITION BY t.SalesOrderLineKey) ELSE 0 END AS MONEY) AS BaseAmountAdj
        , CAST(CASE WHEN t.IsProrateAdj = 1
                    THEN fcl.BaseAmount_TransCur - SUM(t.BaseAmount_TransCur) OVER (PARTITION BY t.SalesOrderLineKey)
                    ELSE 0 END AS MONEY)                                                                                 AS BaseAmount_TransCurAdj
        , CAST(CASE WHEN t.IsProrateAdj = 1
                    THEN fcl.DiscountAmount - SUM(t.DiscountAmount) OVER (PARTITION BY t.SalesOrderLineKey)
                    ELSE 0 END AS MONEY)                                                                                 AS DiscountAmountAdj
        , CAST(CASE WHEN t.IsProrateAdj = 1
                    THEN fcl.DiscountAmount_TransCur
                          - SUM(t.DiscountAmount_TransCur) OVER (PARTITION BY t.SalesOrderLineKey)
                    ELSE 0 END AS MONEY)                                                                                 AS DiscountAmount_TransCurAdj
        , CAST(CASE WHEN t.IsProrateAdj = 1
                    THEN fcl.IncludedCharge - SUM(t.IncludedCharge) OVER (PARTITION BY t.SalesOrderLineKey)
                    ELSE 0 END AS MONEY)                                                                                 AS IncludedChargeAdj
        , CAST(CASE WHEN t.IsProrateAdj = 1
                    THEN fcl.IncludedCharge_TransCur
                          - SUM(t.IncludedCharge_TransCur) OVER (PARTITION BY t.SalesOrderLineKey)
                    ELSE 0 END AS MONEY)                                                                                 AS IncludedCharge_TransCurAdj
        , CAST(CASE WHEN t.IsProrateAdj = 1
                    THEN fcl.AdditionalCharge - SUM(t.AdditionalCharge) OVER (PARTITION BY t.SalesOrderLineKey)
                    ELSE 0 END AS MONEY)                                                                                 AS AdditionalChargeAdj
        , CAST(CASE WHEN t.IsProrateAdj = 1
                    THEN fcl.AdditionalCharge_TransCur
                          - SUM(t.AdditionalCharge_TransCur) OVER (PARTITION BY t.SalesOrderLineKey)
                    ELSE 0 END AS MONEY)                                                                                 AS AdditionalCharge_TransCurAdj
        , CAST(CASE WHEN t.IsProrateAdj = 1
                    THEN fcl.CustomerCharge - SUM(t.CustomerCharge) OVER (PARTITION BY t.SalesOrderLineKey)
                    ELSE 0 END AS MONEY)                                                                                 AS CustomerChargeAdj
        , CAST(CASE WHEN t.IsProrateAdj = 1
                    THEN fcl.CustomerCharge_TransCur
                          - SUM(t.CustomerCharge_TransCur) OVER (PARTITION BY t.SalesOrderLineKey)
                    ELSE 0 END AS MONEY)                                                                                 AS CustomerCharge_TransCurAdj
        , CAST(CASE WHEN t.IsProrateAdj = 1
                    THEN fcl.NonBillableCharge - SUM(t.NonBillableCharge) OVER (PARTITION BY t.SalesOrderLineKey)
                    ELSE 0 END AS MONEY)                                                                                 AS NonBillableChargeAdj
        , CAST(CASE WHEN t.IsProrateAdj = 1
                    THEN fcl.NonBillableCharge_TransCur
                          - SUM(t.NonBillableCharge_TransCur) OVER (PARTITION BY t.SalesOrderLineKey)
                    ELSE 0 END AS MONEY)                                                                                 AS NonBillableCharge_TransCurAdj
        , CAST(CASE WHEN t.IsProrateAdj = 1
                    THEN fcl.RemainingAmount - SUM(t.RemainingAmount) OVER (PARTITION BY t.SalesOrderLineKey)
                    ELSE 0 END AS MONEY)                                                                                 AS RemainingAmountAdj
        , CAST(CASE WHEN t.IsProrateAdj = 1
                    THEN fcl.RemainingAmount_TransCur
                          - SUM(t.RemainingAmount_TransCur) OVER (PARTITION BY t.SalesOrderLineKey)
                    ELSE 0 END AS MONEY)                                                                                 AS RemainingAmount_TransCurAdj
        , CAST(CASE WHEN t.IsProrateAdj = 1
                    THEN fcl.NetAmount - SUM(t.NetAmount) OVER (PARTITION BY t.SalesOrderLineKey) ELSE 0 END AS MONEY)   AS NetAmountAdj
        , CAST(CASE WHEN t.IsProrateAdj = 1
                    THEN fcl.NetAmount_TransCur - SUM(t.NetAmount_TransCur) OVER (PARTITION BY t.SalesOrderLineKey)
                    ELSE 0 END AS MONEY)                                                                                 AS NetAmount_TransCurAdj
        , CAST(CASE WHEN t.IsProrateAdj = 1
                    THEN fcl.OrderedSalesAmount - SUM(t.OrderedSalesAmount) OVER (PARTITION BY t.SalesOrderLineKey)
                    ELSE 0 END AS NUMERIC(28, 12))                                                                       AS OrderedSalesAmountAdj
        , CAST(CASE WHEN t.IsProrateAdj = 1
                    THEN fcl.OrderedSalesAmount_TransCur
                          - SUM(t.OrderedSalesAmount_TransCur) OVER (PARTITION BY t.SalesOrderLineKey)
                    ELSE 0 END AS NUMERIC(28, 12))                                                                       AS OrderedSalesAmount_TransCurAdj
      FROM salesorderlinetrans_factprorate                     t
    INNER JOIN {{ ref('salesorderline_f') }} fcl
        ON fcl.SalesOrderLineKey = t.SalesOrderLineKey
),
salesorderlinetrans_facttransadj AS (
    SELECT SalesOrderLineKey
            , RecID_IT
            , RecID_SL
            , BaseAmountAdj
            , BaseAmount_TransCurAdj
            , DiscountAmountAdj
            , DiscountAmount_TransCurAdj
            , IncludedChargeAdj
            , IncludedCharge_TransCurAdj
            , AdditionalChargeAdj
            , AdditionalCharge_TransCurAdj
            , CustomerChargeAdj
            , CustomerCharge_TransCurAdj
            , NonBillableChargeAdj
            , NonBillableCharge_TransCurAdj
            , RemainingAmountAdj
            , RemainingAmount_TransCurAdj
            , NetAmountAdj
            , NetAmount_TransCurAdj
            , OrderedSalesAmountAdj
            , OrderedSalesAmount_TransCurAdj
          FROM salesorderlinetrans_factadj
        WHERE BaseAmountAdj                  <> 0
            OR BaseAmount_TransCurAdj         <> 0
            OR DiscountAmountAdj              <> 0
            OR DiscountAmount_TransCurAdj     <> 0
            OR IncludedChargeAdj              <> 0
            OR IncludedCharge_TransCurAdj     <> 0
            OR AdditionalChargeAdj            <> 0
            OR AdditionalCharge_TransCurAdj   <> 0
            OR CustomerChargeAdj              <> 0
            OR CustomerCharge_TransCurAdj     <> 0
            OR NonBillableChargeAdj           <> 0
            OR NonBillableCharge_TransCurAdj  <> 0
            OR RemainingAmountAdj             <> 0
            OR RemainingAmount_TransCurAdj    <> 0
            OR NetAmountAdj                   <> 0
            OR NetAmount_TransCurAdj          <> 0
            OR OrderedSalesAmountAdj          <> 0
            OR OrderedSalesAmount_TransCurAdj <> 0;
),
salesorderlinetrans_factprorate2 AS (
    SELECT t.SalesOrderLineKey
        , t.PercentOfTotal
        , t.BaseAmount + ta.BaseAmountAdj                                        As BaseAmount
        , t.BaseAmount_TransCur + ta.BaseAmount_TransCurAdj                      As BaseAmount_TransCur
        , t.DiscountAmount + ta.DiscountAmountAdj                                As DiscountAmount
        , t.DiscountAmount_TransCur + ta.DiscountAmount_TransCurAdj              As DiscountAmount_TransCur
        , t.IncludedCharge + ta.IncludedChargeAdj                                As IncludedCharge
        , t.IncludedCharge_TransCur + ta.IncludedCharge_TransCurAdj              As IncludedCharge_TransCur
        , t.AdditionalCharge + ta.AdditionalChargeAdj                            As AdditionalCharge
        , t.AdditionalCharge_TransCur + ta.AdditionalCharge_TransCurAdj          As AdditionalCharge_TransCur
        , t.CustomerCharge + ta.CustomerChargeAdj                                As CustomerCharge 
        , t.CustomerCharge_TransCur + ta.CustomerCharge_TransCurAdj              As CustomerCharge_TransCur
        , t.NonBillableCharge + ta.NonBillableChargeAdj                          As NonBillableCharge
        , t.NonBillableCharge_TransCur + ta.NonBillableCharge_TransCurAdj        As NonBillableCharge_TransCur
        , t.RemainingAmount + ta.RemainingAmountAdj                              As RemainingAmount
        , t.RemainingAmount_TransCur + ta.RemainingAmount_TransCurAdj            As RemainingAmount_TransCur
        , t.NetAmount + ta.NetAmountAdj                                          As NetAmount
        , t.NetAmount_TransCur + ta.NetAmount_TransCurAdj                        As NetAmount_TransCur
        , t.OrderedSalesAmount + ta.OrderedSalesAmountAdj                        As OrderedSalesAmount
        , t.OrderedSalesAmount_TransCur + ta.OrderedSalesAmount_TransCurAdj      As OrderedSalesAmount_TransCur
        , t.PackingSlipID
        , t.AvailableDate
        , t.RecID_IT
        , t.RecID_SL
        , t.RecID_IB
        , t.STATUSISSUE
        , t.STATUSRECEIPT
        , t.IsProrateAdj
        , t._SourceDate
      FROM salesorderlinetrans_factprorate       t
    INNER JOIN salesorderlinetrans_facttransadj ta
        ON ta.SalesOrderLineKey = t.SalesOrderLineKey
      AND ta.RecID_IT          = t.RecID_IT;
),
salesorderlinetrans_factprorate3 AS (
    Select
            t.SalesOrderLineKey
          , t.PercentOfTotal
          , t.BaseAmount
          , t.BaseAmount_TransCur
          , t.DiscountAmount
          , t.DiscountAmount_TransCur
          , t.IncludedCharge
          , t.IncludedCharge_TransCur
          , t.AdditionalCharge
          , t.AdditionalCharge_TransCur
          , t.CustomerCharge 
          , t.CustomerCharge_TransCur
          , t.NonBillableCharge
          , t.NonBillableCharge_TransCur
          , t.RemainingAmount
          , t.RemainingAmount_TransCur
          , t.NetAmount
          , t.NetAmount_TransCur
          , t.OrderedSalesAmount
          , t.OrderedSalesAmount_TransCur
          , t.PackingSlipID
          , t.AvailableDate
          , t.RecID_IT
          , t.RecID_SL
          , t.RecID_IB
          , t.STATUSISSUE
          , t.STATUSRECEIPT
          , t.IsProrateAdj
          , t._SourceDate
        FROM salesorderlinetrans_factprorate       t
        LEFT JOIN salesorderlinetrans_facttransadj ta
          ON ta.SalesOrderLineKey = t.SalesOrderLineKey
        AND ta.RecID_IT          = t.RecID_IT
        WHERE  ta.SalesOrderLineKey IS NULL
),
salesorderlinetrans_factprorate4 AS (
    Select * from   salesorderlinetrans_factprorate2
    UNION ALL
    Select * from   salesorderlinetrans_factprorate3
),
salesorderlinetrans_factdetail1 AS (
    SELECT fsl.SalesOrderLineKey                                                                                  AS SalesOrderLineKey
      , CONVERT(
            VARCHAR
          , NULLIF(CASE WHEN ds.InventoryTransStatusID IN ( 1, 2 ) THEN it.datestatus ELSE NULL END, '1/1/1900')
          , 112)                                                                                                  AS ShipDateKey
      , CASE WHEN DATEDIFF(d, trd.ReservedDate, dt.ProductionDate) = 0
              THEN 1
              WHEN trd.ReservedDate = '1/1/1900'
                OR dt.ProductionDate = '1/1/1900'
              THEN NULL
              WHEN DATEDIFF(d, trd.ReservedDate, dt.ProductionDate) < 0
              THEN DATEDIFF(d, trd.ReservedDate, dt.ProductionDate) * -1
              ELSE DATEDIFF(d, trd.ReservedDate, dt.ProductionDate) END                                            AS ReservedDays
      , trd.ReservedDate                                                                                           AS ReserveDate
      , ds.InventoryTransStatusKey                                                                                 AS InventoryTransStatusKey
      , dt.TagKey                                                                                                  AS TagKey
      , it.costamountposted * -1                                                                                   AS PostedCost
      , it.costamountadjustment * -1                                                                               AS PostedCostAdjustment
      , it.costamountphysical * -1                                                                                 AS PhysicalCost
      , (it.costamountposted + it.costamountadjustment) * -1                                                       AS FinancialCost
      , fsl.BaseUnitPrice                                                                                          AS BaseUnitPrice
      , fsl.BaseUnitPrice_TransCur                                                                                 AS BaseUnitPrice_TransCur
      , fsl.TotalUnitPrice                                                                                         AS TotalUnitPrice
      , fsl.TotalUnitPrice_TransCur                                                                                AS TotalUnitPrice_TransCur
      , fsl.PriceUnit                                                                                              AS PriceUnit
      , ISNULL(ts.PercentOfTotal, 1)                                                                               AS PercentOfTotal
      , ISNULL(ts.BaseAmount, fsl.BaseAmount)                                                                      AS BaseAmount
      , ISNULL(ts.BaseAmount_TransCur, fsl.BaseAmount_TransCur)                                                    AS BaseAmount_TransCur
      , ISNULL(ts.OrderedSalesAmount, fsl.OrderedSalesAmount)                                                      AS OrderedSalesAmount
      , ISNULL(ts.OrderedSalesAmount_TransCur, fsl.OrderedSalesAmount_TransCur)                                    AS OrderedSalesAmount_TransCur
      , ISNULL(ts.IncludedCharge, fsl.IncludedCharge)                                                              AS IncludedCharge
      , ISNULL(ts.IncludedCharge_TransCur, fsl.IncludedCharge_TransCur)                                            AS IncludedCharge_TransCur
      , ISNULL(ts.AdditionalCharge, fsl.AdditionalCharge)                                                          AS AdditionalCharge
      , ISNULL(ts.AdditionalCharge_TransCur, fsl.AdditionalCharge_TransCur)                                        AS AdditionalCharge_TransCur
      , ISNULL(ts.CustomerCharge, fsl.CustomerCharge)                                                              AS CustomerCharge
      , ISNULL(ts.CustomerCharge_TransCur, fsl.CustomerCharge_TransCur)                                            AS CustomerCharge_TransCur
      , ISNULL(ts.NonBillableCharge, fsl.NonBillableCharge)                                                        AS NonBillableCharge
      , ISNULL(ts.NonBillableCharge_TransCur, fsl.NonBillableCharge_TransCur)                                      AS NonBillableCharge_TransCur
      , ISNULL(ts.DiscountAmount, fsl.DiscountAmount)                                                              AS DiscountAmount
      , ISNULL(ts.DiscountAmount_TransCur, fsl.DiscountAmount_TransCur)                                            AS DiscountAmount_TransCur
      , ISNULL(ts.NetAmount, fsl.NetAmount)                                                                        AS NetAmount
      , ISNULL(ts.NetAmount_TransCur, fsl.NetAmount_TransCur)                                                      AS NetAmount_TransCur
      , ISNULL(tt.OrderedQuantity, 1) * fsl.OrderedQuantity_SalesUOM / ISNULL(NULLIF(fsl.OrderedQuantity, 0), 1)   AS OrderedQuantity_SalesUOM
      , ISNULL(tt.OrderedQuantity, fsl.OrderedQuantity)                                                            AS OrderedQuantity
      , ISNULL(
            tt.OrderedQuantity * fsl.OrderedQuantity_LB / ISNULL(NULLIF(fsl.OrderedQuantity, 0), 1)
          , fsl.OrderedQuantity_LB)                                                                                AS OrderedQuantity_LB
      , ISNULL(
            tt.OrderedQuantity * fsl.OrderedQuantity_PC / ISNULL(NULLIF(fsl.OrderedQuantity, 0), 1)
          , fsl.OrderedQuantity_PC)                                                                                AS OrderedQuantity_PC
      , ISNULL(
            tt.OrderedQuantity * fsl.OrderedQuantity_FT / ISNULL(NULLIF(fsl.OrderedQuantity, 0), 1)
          , fsl.OrderedQuantity_FT)                                                                                AS OrderedQuantity_FT
      , ISNULL(
            tt.OrderedQuantity * fsl.OrderedQuantity_SQIN / ISNULL(NULLIF(fsl.OrderedQuantity, 0), 1)
          , fsl.OrderedQuantity_SQIN)                                                                              AS OrderedQuantity_SQIN
      , ISNULL(tu.ShippedQuantity_SalesUOM, fsl.ShippedQuantity_SalesUOM) * fsl.TotalUnitPrice
        / ISNULL(NULLIF(fsl.PriceUnit, 0), 1)                                                                      AS ShippedAmount
      , ISNULL(tu.ShippedQuantity_SalesUOM, fsl.ShippedQuantity_SalesUOM) * fsl.TotalUnitPrice_TransCur
        / ISNULL(NULLIF(fsl.PriceUnit, 0), 1)                                                                      AS ShippedAmount_TransCur
      , ISNULL(tt.ShippedQuantity, 1) * fsl.ShippedQuantity_SalesUOM / ISNULL(NULLIF(fsl.ShippedQuantity, 0), 1)   AS ShippedQuantity_SalesUOM
      , ISNULL(tt.ShippedQuantity, fsl.ShippedQuantity)                                                            AS ShippedQuantity
      , ISNULL(tt.ShippedQuantity, 1) * fsl.ShippedQuantity_LB / ISNULL(NULLIF(fsl.ShippedQuantity, 0), 1)         AS ShippedQuantity_LB
      , ISNULL(tt.ShippedQuantity, 1) * fsl.ShippedQuantity_PC / ISNULL(NULLIF(fsl.ShippedQuantity, 0), 1)         AS ShippedQuantity_PC
      , ISNULL(tt.ShippedQuantity, 1) * fsl.ShippedQuantity_FT / ISNULL(NULLIF(fsl.ShippedQuantity, 0), 1)         AS ShippedQuantity_FT
      , ISNULL(tt.ShippedQuantity, 1) * fsl.ShippedQuantity_SQIN / ISNULL(NULLIF(fsl.ShippedQuantity, 0), 1)       AS ShippedQuantity_SQIN
      , ISNULL(ts.RemainingAmount, fsl.RemainingAmount)                                                            AS RemainingAmount
      , ISNULL(ts.RemainingAmount_TransCur, fsl.RemainingAmount_TransCur)                                          AS RemainingAmount_TransCur
      , ISNULL(tt.RemainingQuantity, 1) * fsl.RemainingQuantity_SalesUOM
        / ISNULL(NULLIF(fsl.RemainingQuantity, 0), 1)                                                              AS RemainingQuantity_SalesUOM
      , ISNULL(tt.RemainingQuantity, fsl.RemainingQuantity)                                                        AS RemainingQuantity
      , ISNULL(tt.RemainingQuantity, 1) * fsl.RemainingQuantity_LB / ISNULL(NULLIF(fsl.RemainingQuantity, 0), 1)   AS RemainingQuantity_LB
      , ISNULL(tt.RemainingQuantity, 1) * fsl.RemainingQuantity_PC / ISNULL(NULLIF(fsl.RemainingQuantity, 0), 1)   AS RemainingQuantity_PC
      , ISNULL(tt.RemainingQuantity, 1) * fsl.RemainingQuantity_FT / ISNULL(NULLIF(fsl.RemainingQuantity, 0), 1)   AS RemainingQuantity_FT
      , ISNULL(tt.RemainingQuantity, 1) * fsl.RemainingQuantity_SQIN / ISNULL(NULLIF(fsl.RemainingQuantity, 0), 1) AS RemainingQuantity_SQIN
      , tuc.PhysicalReservedQuantity                                                                               AS PhysicalReservedQuantity
      , tuc.PhysicalReservedQuantity_SalesUOM                                                                      AS PhysicalReservedQuantity_SalesUOM
      , tuc.PhysicalReservedQuantity_FT                                                                            AS PhysicalReservedQuantity_FT
      , tuc.PhysicalReservedQuantity_LB                                                                            AS PhysicalReservedQuantity_LB
      , tuc.PhysicalReservedQuantity_PC                                                                            AS PhysicalReservedQuantity_PC
      , ts.PackingSlipID                                                                                           AS PackingSlipID
      , ts.AvailableDate                                                                                           AS AvailableDate
      , ts._SourceDate
      , ISNULL(ts.RecID_IT, 0)                                                                                     AS _RecID2
      , fsl._RecID                                                                                                 AS _RECID1
      , 1                                                                                                          AS _SourceID
    FROM {{ ref('salesorderline_f') }}       fsl
    LEFT JOIN salesorderlinetrans_factprorate4                 ts
      ON ts.SalesOrderLineKey          = fsl.SalesOrderLineKey
    LEFT JOIN salesorderlinetrans_factreservedaging           trd
      ON trd.RecID_IT                  = ts.RecID_IT
    LEFT JOIN salesorderlinetrans_facttrans                   tt
      ON tt.RecID_IT                   = ts.RecID_IT
    LEFT JOIN salesorderlinetrans_factuomconversion           tuc
      ON tuc._RecID                    = ts.RecID_IT
    LEFT JOIN salesorderlinetrans_facttransuom                tu
      ON tu.RecID_IT                   = ts.RecID_IT
    LEFT JOIN {{ ref('tag_d') }}                  dt
      ON dt._RecID                     = ts.RecID_IB
    AND dt._SourceID                  = 1
    LEFT JOIN {{ ref('inventtrans') }}          it
      ON it.recid                      = ts.RecID_IT
    LEFT JOIN {{ ref('inventory_trans_status_d') }} ds
      ON ds.InventoryTransStatusID     = CASE WHEN ts.STATUSISSUE > 0 THEN ts.STATUSISSUE ELSE ts.STATUSRECEIPT END
    AND ds.InventoryTransStatusTypeID = CASE WHEN ts.STATUSISSUE > 0 THEN 1 ELSE 2 END;
)
SELECT ROW_NUMBER() OVER (ORDER BY td._RECID1) AS SalesOrderLineTransKey
    , td.SalesOrderLineKey                     AS SalesOrderLineKey
    , td.ReserveDate                           AS ReserveDate
    , td.ReservedDays                          AS DaysReserved
    , ab.AgingBucketKey                        AS AgingReservedBucketKey
    , td.ShipDateKey                           AS ShipDateKey
    , td.InventoryTransStatusKey               AS InventoryTransStatusKey
    , td.TagKey                                AS TagKey
    , td.PostedCost                            AS PostedCost
    , td.PostedCostAdjustment                  AS PostedCostAdjustment
    , td.PhysicalCost                          AS PhysicalCost
    , td.FinancialCost                         AS FinancialCost
    , td.BaseUnitPrice                         AS BaseUnitPrice
    , td.BaseUnitPrice_TransCur                AS BaseUnitPrice_TransCur
    , td.TotalUnitPrice                        AS TotalUnitPrice
    , td.TotalUnitPrice_TransCur               AS TotalUnitPrice_TransCur
    , td.PriceUnit                             AS PriceUnit
    , td.PercentOfTotal                        AS PercentOfTotal
    , td.BaseAmount                            AS BaseAmount
    , td.BaseAmount_TransCur                   AS BaseAmount_TransCur
    , td.OrderedSalesAmount                    AS OrderedSalesAmount
    , td.OrderedSalesAmount_TransCur           AS OrderedSalesAmount_TransCur
    , td.IncludedCharge                        AS IncludedCharge
    , td.IncludedCharge_TransCur               AS IncludedCharge_TransCur
    , td.AdditionalCharge                      AS AdditionalCharge
    , td.AdditionalCharge_TransCur             AS AdditionalCharge_TransCur
    , td.CustomerCharge                        AS CustomerCharge
    , td.CustomerCharge_TransCur               AS CustomerCharge_TransCur
    , td.NonBillableCharge                     AS NonBillableCharge
    , td.NonBillableCharge_TransCur            AS NonBillableCharge_TransCur
    , td.DiscountAmount                        AS DiscountAmount
    , td.DiscountAmount_TransCur               AS DiscountAmount_TransCur
    , td.NetAmount                             AS NetAmount
    , td.NetAmount_TransCur                    AS NetAmount_TransCur
    , td.OrderedQuantity_SalesUOM              AS OrderedQuantity_SalesUOM
    , td.OrderedQuantity                       AS OrderedQuantity
    , td.OrderedQuantity_LB                    AS OrderedQuantity_LB
    , td.OrderedQuantity_PC                    AS OrderedQuantity_PC
    , td.OrderedQuantity_FT                    AS OrderedQuantity_FT
    , td.OrderedQuantity_SQIN                  AS OrderedQuantity_SQIN
    , td.ShippedAmount                         AS ShippedAmount
    , td.ShippedAmount_TransCur                AS ShippedAmount_TransCur
    , td.ShippedQuantity_SalesUOM              AS ShippedQuantity_SalesUOM
    , td.ShippedQuantity                       AS ShippedQuantity
    , td.ShippedQuantity_LB                    AS ShippedQuantity_LB
    , td.ShippedQuantity_PC                    AS ShippedQuantity_PC
    , td.ShippedQuantity_FT                    AS ShippedQuantity_FT
    , td.ShippedQuantity_SQIN                  AS ShippedQuantity_SQIN
    , td.RemainingAmount                       AS RemainingAmount
    , td.RemainingAmount_TransCur              AS RemainingAmount_TransCur
    , td.RemainingQuantity_SalesUOM            AS RemainingQuantity_SalesUOM
    , td.RemainingQuantity                     AS RemainingQuantity
    , td.RemainingQuantity_LB                  AS RemainingQuantity_LB
    , td.RemainingQuantity_PC                  AS RemainingQuantity_PC
    , td.RemainingQuantity_FT                  AS RemainingQuantity_FT
    , td.RemainingQuantity_SQIN                AS RemainingQuantity_SQIN
    , td.PhysicalReservedQuantity              AS PhysicalReservedQuantity
    , td.PhysicalReservedQuantity_SalesUOM     AS PhysicalReservedQuantity_SalesUOM
    , td.PhysicalReservedQuantity_FT           AS PhysicalReservedQuantity_FT
    , td.PhysicalReservedQuantity_LB           AS PhysicalReservedQuantity_LB
    , td.PhysicalReservedQuantity_PC           AS PhysicalReservedQuantity_PC
    , td.PackingSlipID                         AS PackingSlipID
    , td.AvailableDate                         AS AvailableDate
    , td._SourceDate
    , td._RECID1                               AS _RECID1
    , td._RecID2                               AS _RecID2
    , td._SourceID                             AS _SourceID
    , cast(CURRENT_TIMESTAMP as DATETIME2(6))                        AS  _CreatedDate  
    , cast(CURRENT_TIMESTAMP as DATETIME2(6))                        AS _ModifiedDate
  FROM salesorderlinetrans_factdetail1             td
  LEFT JOIN {{ ref('agingbucket_d') }} ab
    ON td.ReservedDays BETWEEN ab.AgeDaysBegin AND ab.AgeDaysEnd;
