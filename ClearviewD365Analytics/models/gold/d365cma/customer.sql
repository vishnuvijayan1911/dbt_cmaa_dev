{{ config(materialized='view', schema='gold', alias="Customer") }}

WITH CTE
  AS (
    SELECT  t.CustomerKey
          , MIN(dd.Date)                         AS CustomerFirstOrderDate
          , MAX(dd.Date)                         AS CustomerLastOrderDate
          , DATEDIFF(d, MIN(dd.Date), GETDATE()) AS CustomerDaysSinceFirstOrder
          , DATEDIFF(d, MAX(dd.Date), GETDATE()) AS CustomerDaysSinceLastOrder
      FROM {{ ref("customer") }}             t
      LEFT JOIN {{ ref("salesorder_fact") }} sil
        ON sil.CustomerKey = t.CustomerKey
      LEFT JOIN {{ ref('date') }}            dd
        ON dd.DateKey      = sil.OrderDateKey
      GROUP BY t.CustomerKey)
SELECT  t.CustomerKey                                                                                   AS [Customer key]
    , F.LegalEntityKey                                                                                AS [Legal entity key]
    , NULLIF(t.Customer, '')                                                                          AS [Customer]
    , CASE WHEN t.CustomerKey <> -1 THEN CAST(1 AS SMALLINT)ELSE NULL END                             AS [Customers]
    , CAST(1 AS INT)                                                                                  AS [Customer count]
    , NULLIF(t.CustomerAccount, '')                                                                   AS [Customer #]
    , NULLIF(LTRIM(t.CustomerName), '')                                                               AS [Customer name]
    , NULLIF(A.City, '')                                                                              AS [Customer city]
    , NULLIF(A.CountryID, '')                                                                         AS [Customer country]
    , NULLIF(A.Country, '')                                                                           AS [Customer country name]
    , NULLIF(dm.DeliveryModeID, '')                                                                   AS [Customer delivery mode]
    , NULLIF(dm.DeliveryMode, '')                                                                     AS [Customer delivery mode name]
    , NULLIF(dt.DeliveryTermID, '')                                                                   AS [Customer delivery term]
    , NULLIF(dt.DeliveryTerm, '')                                                                     AS [Customer delivery term name]
    , NULLIF(pm.PaymentModeID, '')                                                                    AS [Customer payment mode]
    , NULLIF(pm.PaymentMode, '')                                                                      AS [Customer payment mode name]
    , NULLIF(pt.PaymentTermID, '')                                                                    AS [Customer payment term]
    , NULLIF(pt.PaymentTerm, '')                                                                      AS [Customer payment term name]
    , ISNULL(NULLIF(g.CustomerGroupID, ''), 'Other')                                                  AS [Customer group]
    , ISNULL(NULLIF(g.CustomerGroup, ''), 'Other')                                                    AS [Customer group name]
    , ISNULL(F.OpenBalance, 0)                                                                        AS [Open balance]
    , NULLIF(t.CustomerAlias, '')                                                                     AS [Customer alias]
    , NULLIF(t.OnHoldStatus, '')                                                                      AS [Customer on-hold status]
    , NULLIF(A.StateProvince, '')                                                                     AS [Customer state province]
    , NULLIF(A.Street, '')                                                                            AS [Customer street]
    , NULLIF(A.PostalCode, '')                                                                        AS [Customer postal code]
    , NULLIF(t.SalesDistrict, '')                                                                     AS [Sales district]
    , CASE WHEN g.CustomerGroup LIKE 'Intercompany' THEN 'Intercompany' ELSE 'External' END           AS [Customer type]
    , NULLIF(F.CreditLimit, 0)                                                                        AS [Credit limit]
    , NULLIF(F.OverCreditLimit, 0)                                                                    AS [Over credit limit]
    , CASE WHEN ISNULL(F.CreditLimit, 0) = 0
            THEN 'No limit defined'
            ELSE CASE WHEN F.OverCreditLimit > 0 THEN 'Over credit limit' ELSE 'Unused credit' END END AS [Credit status]
    , CASE WHEN F.OverCreditLimit > 0 AND ISNULL(F.CreditLimit, 0) <> 0 THEN 1 ELSE NULL END          AS [Customers over credit limit]
    , NULLIF(F.RemainingCredit, 0)                                                                    AS [Remaining credit]
    , CustomerFirstOrderDate                                                                          AS [Customer first order date]
    , CustomerLastOrderDate                                                                           AS [Customer last order date]
    , CustomerDaysSinceFirstOrder                                                                     AS [Customer days since first order]
    , CustomerDaysSinceLastOrder                                                                      AS [Customer days since last order]
  FROM {{ ref("customer") }}            t 
  LEFT JOIN {{ ref("customer_fact") }}  F 
    ON F.CustomerKey         = t.CustomerKey
  LEFT JOIN {{ ref("address") }}        A 
    ON A.AddressKey          = F.AddressKey
  LEFT JOIN {{ ref("customergroup") }}  g 
    ON g.CustomerGroupKey    = F.CustomerGroupKey
  LEFT JOIN {{ ref("paymentterm") }}    pt 
    ON pt.PaymentTermKey     = F.PaymentTermKey
  LEFT JOIN {{ ref("deliveryterm") }}   dt 
    ON dt.DeliveryTermKey    = F.DeliveryTermKey
  LEFT JOIN {{ ref("paymentmode") }}    pm 
    ON pm.PaymentModeKey     = F.PaymentModeKey
  LEFT JOIN {{ ref("deliverymode") }}   dm 
    ON dm.DeliveryModeKey    = F.DeliveryModeKey
  LEFT JOIN CTE                cte
    ON cte.CustomerKey       = t.CustomerKey;
