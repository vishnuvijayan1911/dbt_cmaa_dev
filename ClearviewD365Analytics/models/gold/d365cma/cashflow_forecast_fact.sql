{{ config(materialized='view', schema='gold', alias="Cashflow forecast fact") }}

SELECT  t.*
FROM (   SELECT  le.LegalEntityKey                      AS [Legal entity key]
              , 'Sales order'                              AS [Type]
              , dc.CustomerKey                             AS [Customer key]
              , 6326                                       AS [Ledger account key]
              , -1                                         AS [Product key]
              , -1                                         AS [Purchase order key]
              , -1                                         AS [Purchase order line key]
              , -1                                         AS [Purchase invoice key]
              , solf.SalesOrderKey                         AS [Sales order key]
              , solf.SalesOrderLineKey                     AS [Sales order line key]
              , -1                                         AS [Sales invoice key]
              , -1                                         AS [Sales person key]
              , -1                                         AS [Vendor key]
              , sol.SalesOrderID                           AS [Reference #]
              , dc.CustomerAccount                         AS [Reference account]
              , pt.PaymentDays                             AS [Payment term days]
              , solf.ShipDateDueKey                        AS [Order date key]
              , CASE WHEN solf.ReceiptDateConfirmedKey IS NOT NULL
                      AND solf.ReceiptDateConfirmedKey <> 19000101
                    THEN solf.ReceiptDateConfirmedKey
                    ELSE solf.ReceiptDateRequestedKey END AS [Invoice date key]
              , CASE WHEN dd1.Date IS NOT NULL
                      AND dd1.Date <> '19000101'
                    THEN CASE WHEN dd1.Date > GETDATE()
                              THEN FORMAT(DATEADD(d, ISNULL(pt.PaymentDays, 0), GETDATE()), 'yyyyMMdd')
                              ELSE FORMAT(DATEADD(d, ISNULL(pt.PaymentDays, 0), dd1.Date), 'yyyyMMdd') END
                    WHEN dd2.Date IS NOT NULL
                      AND dd2.Date <> '19000101'
                    THEN CASE WHEN dd2.Date > GETDATE()
                              THEN FORMAT(DATEADD(d, ISNULL(pt.PaymentDays, 0), GETDATE()), 'yyyyMMdd')
                              ELSE FORMAT(DATEADD(d, ISNULL(pt.PaymentDays, 0), dd2.Date), 'yyyyMMdd') END
                    ELSE dt1.DateKey END                  AS [Settlement date key]
              , solf.NetAmount                             AS [Amount]
          FROM {{ ref("salesorderline_f") }} solf
          INNER JOIN {{ ref("salesorderline_d") }} sol
            ON sol.SalesOrderLineKey = solf.SalesOrderLineKey
          INNER JOIN {{ ref("legalentity_d") }} le
            ON le.LegalEntityKey = solf.LegalEntityKey
          LEFT JOIN {{ ref('date_d') }}           dd1
            ON dd1.DateKey           = solf.ReceiptDateConfirmedKey
          LEFT JOIN {{ ref('date_d') }}           dd2
            ON dd2.DateKey           = solf.ReceiptDateRequestedKey
          INNER JOIN {{ ref("paymentterm_d") }}    pt
            ON pt.PaymentTermKey     = solf.PaymentTermKey
          INNER JOIN {{ ref("salesstatus_d") }}    st
            ON st.SalesStatusKey     = solf.SalesLineStatusKey
          INNER JOIN {{ ref("customer_d") }}       dc
            ON dc.CustomerKey        = solf.CustomerKey
          LEFT JOIN {{ ref('date_d') }}           dt
            ON dt.Date               = CAST(GETDATE() AS DATE)
          LEFT JOIN {{ ref('date_d') }}           dt1
            ON dt.WeekDate           = dt1.Date
          WHERE st.SalesStatusID = '1'
            AND solf.NetAmount   <> 0
        UNION
        SELECT  le.LegalEntityKey                       AS [Legal entity key]
              , 'Purchase order'                            AS [Type]
              , -1                                          AS [Customer key]
              , 1818                                        AS [Ledger account key]
              , -1                                          AS [Product key]
              , polf.PurchaseOrderKey                       AS [Purchase order key]
              , polf.PurchaseOrderLineKey                   AS [Purchase order line key]
              , -1                                          AS [Purchase invoice key]
              , -1                                          AS [Sales order key]
              , -1                                          AS [Sales order line key]
              , -1                                          AS [Sales invoice key]
              , -1                                          AS [Sales person key]
              , dv.VendorKey                                AS [Vendor key]
              , pol.PurchaseOrderID                         AS [Reference #]
              , dv.VendorAccount                            AS [Reference account]
              , pt.PaymentDays                              AS [Payment term days]
              , polf.DeliveryDateKey                        AS [Order date key]
              , CASE WHEN polf.DeliveryDateActualKey IS NOT NULL
                      AND polf.DeliveryDateActualKey <> 19000101
                    THEN polf.DeliveryDateActualKey
                    ELSE polf.DeliveryDateConfirmedKey END AS [Invoice date key]
              , CASE WHEN dd1.Date IS NOT NULL
                      AND dd1.Date <> '1900-01-01'
                    THEN CASE WHEN dd1.Date > GETDATE()
                              THEN FORMAT(DATEADD(d, ISNULL(pt.PaymentDays, 0), GETDATE()), 'yyyyMMdd')
                              ELSE FORMAT(DATEADD(d, ISNULL(pt.PaymentDays, 0), dd1.Date), 'yyyyMMdd') END
                    WHEN dd2.Date IS NOT NULL
                      AND dd2.Date <> '1900-01-01'
                    THEN CASE WHEN dd2.Date > GETDATE()
                              THEN FORMAT(DATEADD(d, ISNULL(pt.PaymentDays, 0), GETDATE()), 'yyyyMMdd')
                              ELSE FORMAT(DATEADD(d, ISNULL(pt.PaymentDays, 0), dd2.Date), 'yyyyMMdd') END
                    ELSE dt1.DateKey END                   AS [Settlement date key]
              , polf.NetAmount * -1                         AS [Amount]
          FROM {{ ref("purchaseorderline_f") }} polf
          INNER JOIN {{ ref("purchaseorderline_d") }} pol
            ON pol.PurchaseOrderLineKey = polf.PurchaseOrderLineKey
          INNER JOIN {{ ref("legalentity_d") }}    le
            ON le.LegalEntityKey    = polf.LegalEntityKey
          INNER JOIN {{ ref('date_d') }}              dd1
            ON dd1.DateKey              = polf.DeliveryDateActualKey
          INNER JOIN {{ ref('date_d') }}              dd2
            ON dd2.DateKey              = polf.DeliveryDateConfirmedKey
          INNER JOIN {{ ref("paymentterm_d") }}       pt
            ON pt.PaymentTermKey        = polf.PaymentTermKey
          INNER JOIN {{ ref("purchasestatus_d") }}    st
            ON st.PurchaseStatusKey     = polf.PurchaseLineStatusKey
          INNER JOIN {{ ref("vendor_d") }}            dv
            ON dv.VendorKey             = polf.VendorKey
          LEFT JOIN {{ ref('date_d') }}              dt
            ON dt.Date                  = CAST(GETDATE() AS DATE)
          LEFT JOIN {{ ref('date_d') }}              dt1
            ON dt.WeekDate              = dt1.Date
          WHERE st.PurchaseStatusID = '1'
            AND polf.NetAmount      <> 0
    UNION
        SELECT  bt.LegalEntityKey   AS [Legal entity key]
              , 'General ledger'    AS TYPE
              , -1                  AS [Customer key]
              , bt.LedgerAccountKey AS [Ledger account key]
              , -1                  AS [Product key]
              , -1                  AS [Purchase order key]
              , -1                  AS [Purchase order line key]
              , -1                  AS [Purchase invoice key]
              , -1                  AS [Sales order key]
              , -1                  AS [Sales order line key]
              , -1                  AS [Sales invoice key]
              , -1                  AS [Sales person key]
              , -1                  AS [Vendor key]
              , bt.BudgetNumber     AS [Reference #]
              , ''                  AS [Reference account]
              , NULL                AS [Payment term days]
              , '19000101'          AS [Order date key]
              , '19000101'          AS [Invoice date key]
              , bt.TransDateKey     AS [Settlement date key]
              , bt.BudgetAmount     AS Amount
          FROM {{ ref("glbudgettrans_f") }}  bt
          INNER JOIN {{ ref("legalentity_d") }}    le
            ON le.LegalEntityKey = bt.LegalEntityKey
          LEFT JOIN {{ ref("ledgeraccount_d") }}  la
            ON la.LedgerAccountKey   = bt.LedgerAccountKey
        UNION
        SELECT  sf.LegalEntityKey  AS [Legal entity key]
              , 'Sales forecast'   AS TYPE
              , -1                 AS [Customer key]
              , 6326               AS [Ledger account key]
              , sf.ProductKey      AS [Product key]
              , -1                 AS [Purchase order key]
              , -1                 AS [Purchase order line key]
              , -1                 AS [Purchase invoice key]
              , -1                 AS [Sales order key]
              , -1                 AS [Sales order line key]
              , -1                 AS [Sales invoice key]
              , sf.SalesPersonKey  AS [Sales person key]
              , -1                 AS [Vendor key]
              , ''                 AS [Reference #]
              , ''                 AS [Reference account]
              , NULL               AS [Payment days]
              , sf.ForecastDateKey AS [Order date key]
              , 19000101           AS [Invoice date key]
              , sf.ForecastDateKey AS [Settlement date key]
              , sf.ForecastAmount  AS Amount
          FROM {{ ref("salesforecast_f") }}  sf
           INNER JOIN {{ ref("legalentity_d") }}    le
            ON le.LegalEntityKey = sf.LegalEntityKey
        UNION
        SELECT  pf.LegalEntityKey      AS [Legal entity key]
              , 'Purchase forecast'    AS TYPE
              , -1                     AS [Customer key]
              , 1818                   AS [Ledger account key]
              , pf.ProductKey          AS [Product key]
              , -1                     AS [Purchase order key]
              , -1                     AS [Purchase order line key]
              , -1                     AS [Purchase invoice key]
              , -1                     AS [Sales order key]
              , -1                     AS [Sales order line key]
              , -1                     AS [Sales invoice key]
              , -1                     AS [Sales person key]
              , -1                     AS [Vendor key]
              , ''                     AS [Reference #]
              , ''                     AS [Reference account]
              , NULL                   AS [Payment days]
              , pf.ForecastDateKey     AS [Order date key]
              , 19000101               AS [Invoice date key]
              , pf.ForecastDateKey     AS [Settlement date key]
              , pf.ForecastAmount * -1 AS Amount
          FROM {{ ref("purchaseforecast_f") }} pf
           INNER JOIN {{ ref("legalentity_d") }}    le
            ON le.LegalEntityKey = pf.LegalEntityKey) t;
