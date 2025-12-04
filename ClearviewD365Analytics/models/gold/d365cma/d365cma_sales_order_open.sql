{{ config(materialized='view', schema='gold', alias="Sales order (open)") }}

WITH CTE
  AS (
    SELECT  CASE WHEN PriororderDate IS NULL THEN NULL ELSE DATEDIFF(d, dd1.Date, dd.Date) END AS CustomerDaysBetweenOrders
          , t.*
      FROM (   SELECT  ROW_NUMBER() OVER (PARTITION BY CustomerKey
ORDER BY OrderDateKey, SalesOrderKey) AS [CustomerOrderSeqNo]
                    , LAG(OrderDateKey) OVER (PARTITION BY CustomerKey
ORDER BY OrderDateKey, SalesOrderKey) AS PriororderDate
                    , OrderDateKey
                    , CustomerKey
                    , SalesOrderKey
                  FROM {{ ref("d365cma_salesorder_f") }}  sof
                  LEFT JOIN {{ ref("d365cma_salesstatus_d") }} ss1 
                    ON ss1.SalesStatusKey = sof.SalesStatusKey
                WHERE ss1.SalesStatusID = 1) t
      LEFT JOIN {{ ref('d365cma_date_d') }}                     dd
        ON dd.DateKey  = t.OrderDateKey
      LEFT JOIN {{ ref('d365cma_date_d') }}                     dd1
        ON dd1.DateKey = t.PriororderDate)
SELECT  t.SalesOrderKey                            AS [Sales order key]
    , NULLIF(t.SalesOrderID, '')                 AS [Sales order #]
    , CustomerOrderSeqNo                         AS [Customer order seq #]
    , NULLIF(t.CustomerReference, '')            AS [Customer reference]
    , NULLIF(t.CustomerRequisition, '')          AS [Customer requisition]
    , 1                                          AS [Order count]
    , NULLIF(ss1.SalesStatus, '')                AS [Order status]
    , NULLIF(ots.OnTimeShipStatus, '')           AS [On-time ship status]
    , NULLIF(ots.OnTimeStatus, '')               AS [On-time status]
    , NULLIF(rs.ReturnStatus, '')                AS [Return status]
    , NULLIF(e4.EmployeeName, '')                AS [Sales taker]
    , NULLIF(st.SalesType, '')                   AS [Sales type]
    , NULLIF(ots.ShipStatus, '')                 AS [Ship status]
    , NULLIF(tg.TaxGroup, '')                    AS [Tax group]
    , NULLIF(c.CurrencyID, '')                   AS [Trans currency]
    , cte.CustomerDaysBetweenOrders              AS [Customer days between orders]
    , CAST(NULLIF(dd1.Date, '1/1/1900') AS DATE) AS [Order date]
    , CAST(NULLIF(dd2.Date, '1/1/1900') AS DATE) AS [Receipt date confirmed]
    , CAST(NULLIF(dd3.Date, '1/1/1900') AS DATE) AS [Receipt date requested]
    , CAST(NULLIF(dd4.Date, '1/1/1900') AS DATE) AS [Ship date actual]
  FROM {{ ref("d365cma_salesorder_d") }}            t 
  JOIN {{ ref("d365cma_salesorder_f") }}       f 
    ON t.SalesOrderKey         = f.SalesOrderKey
  LEFT JOIN {{ ref("d365cma_deliverymode_d") }}     dm 
    ON dm.DeliveryModeKey      = f.DeliveryModeKey
  LEFT JOIN {{ ref("d365cma_deliveryterm_d") }}     dt 
    ON dt.DeliveryTermKey      = f.DeliveryTermKey
  LEFT JOIN {{ ref("d365cma_taxgroup_d") }}         tg 
    ON tg.TaxGroupKey          = f.TaxGroupKey
  LEFT JOIN {{ ref("d365cma_currency_d") }}         c 
    ON c.CurrencyKey           = f.CurrencyKey
  LEFT JOIN {{ ref("d365cma_documentstatus_d") }}   ds 
    ON ds.DocumentStatusKey    = f.DocumentStatusKey
  LEFT JOIN {{ ref("d365cma_salesstatus_d") }}      ss1 
    ON ss1.SalesStatusKey      = f.SalesStatusKey
  LEFT JOIN {{ ref("d365cma_paymentterm_d") }}      pat 
    ON pat.PaymentTermKey      = f.PaymentTermKey
  LEFT JOIN {{ ref("d365cma_returnstatus_d") }}     rs 
    ON rs.ReturnStatusKey      = f.ReturnStatusKey
  LEFT JOIN {{ ref("d365cma_salestype_d") }}        st 
    ON st.SalesTypeKey         = f.SalesTypeKey
  LEFT JOIN {{ ref("d365cma_employee_d") }}         e4 
    ON e4.EmployeeKey          = f.SalesTakerKey
  LEFT JOIN {{ ref('d365cma_date_d') }}             dd1 
    ON dd1.DateKey             = f.OrderDateKey
  LEFT JOIN {{ ref('d365cma_date_d') }}             dd2 
    ON dd2.DateKey             = f.ReceiptDateConfirmedKey
  LEFT JOIN {{ ref('d365cma_date_d') }}             dd3 
    ON dd3.DateKey             = f.ReceiptDateRequestedKey
  LEFT JOIN {{ ref('d365cma_date_d') }}             dd4 
    ON dd4.DateKey             = f.ShipDateActualKey
  LEFT JOIN {{ ref("d365cma_paymentmode_d") }}      pam 
    ON pam.PaymentModeKey      = f.PaymentModeKey
  LEFT JOIN {{ ref("d365cma_ontimeshipstatus_d") }} ots
    ON ots.OnTimeShipStatusKey = f.OnTimeShipStatusKey
  LEFT JOIN CTE                  cte
    ON cte.SalesOrderKey       = t.SalesOrderKey
WHERE ss1.SalesStatusID <> 4;
