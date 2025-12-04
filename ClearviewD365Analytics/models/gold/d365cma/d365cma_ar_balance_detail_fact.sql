{{ config(materialized='view', schema='gold', alias="AR balance detail fact") }}

SELECT  t.ARBalanceKey                                                                     AS [AR balance key]
    , CAST(1 AS INT)                                                                     AS [AR balance count]
    , CONCAT(
          CASE WHEN t.AgingBucketInvoiceKey < 10
                THEN '00' + CAST(t.AgingBucketInvoiceKey AS VARCHAR(10))
                WHEN t.AgingBucketInvoiceKey < 100
                THEN '0' + CAST(t.AgingBucketInvoiceKey AS VARCHAR(10))
                ELSE CAST(t.AgingBucketInvoiceKey AS VARCHAR(10))END
        , CASE WHEN t.AgingBucketDueKey < 10
                THEN '00' + CAST(t.AgingBucketDueKey AS VARCHAR(10))
                WHEN t.AgingBucketDueKey < 100
                THEN '0' + CAST(t.AgingBucketDueKey AS VARCHAR(10))
                ELSE CAST(t.AgingBucketDueKey AS VARCHAR(10))END)                         AS [Aging bucket key]
    , t.BalanceDateKey                                                                   AS [Balance date key]
    , t.CustomerKey                                                                      AS [Invoice customer key]
    , ISNULL(NULLIF(si.CustomerKey, -1), t.CustomerKey)                                  AS [Ship-to customer key]
    , t.DueDateKey                                                                       AS [Due date key]
    , t.FinancialKey                                                                     AS [Financial key]
    , t.InvoiceDateKey                                                                   AS [Invoice date key]
    , t.LegalEntityKey                                                                   AS [Legal entity key]
    , t.SalesInvoiceKey                                                                  AS [Sales invoice key]
    , t.VoucherKey                                                                       AS [Voucher key]
    , t.ActivityAmount                                                                   AS [Activity amount]
    , t.OpeningBalance                                                                   AS [Opening balance]
    , t.ClosingBalance                                                                   AS [Closing balance]
    , CASE WHEN t.ClosingBalance = 0 THEN NULL ELSE DATEDIFF(DAY, dd1.Date, dd.Date) END AS [AR age by invoice date]
    , CASE WHEN t.ClosingBalance = 0 THEN NULL ELSE DATEDIFF(DAY, dd2.Date, dd.Date) END AS [AR age by due date]
    , NULLIF(t.DiscountAvailable, 0)                                                     AS [Discount available]
    , NULLIF(t.DiscountLost, 0)                                                          AS [Discount lost]
    , NULLIF(t.DiscountTaken, 0)                                                         AS [Discount taken]
    , t.PaymentDays                                                                      AS [Payment days]
    , CAST(NULLIF(dd3.Date, '1/1/1900') AS DATE)                                         AS [Discount date]
  FROM {{ ref("d365cma_arbalancedetail_f") }}   t 
INNER JOIN {{ ref('d365cma_date_d') }}              dd
    ON dd.DateKey             = t.BalanceDateKey
  LEFT JOIN {{ ref('d365cma_date_d') }}              dd1
    ON dd1.DateKey            = t.InvoiceDateKey
  LEFT JOIN {{ ref('d365cma_date_d') }}              dd2
    ON dd2.DateKey            = t.DueDateKey
  LEFT JOIN {{ ref('d365cma_date_d') }}              dd3
    ON dd3.DateKey            = t.DiscDateKey
INNER JOIN {{ ref("d365cma_agingbucket_d") }}       ab
    ON ab.AgingBucketKey      = t.AgingBucketDueKey
  LEFT JOIN {{ ref("d365cma_salesinvoice_f") }} si
    ON si.SalesInvoiceKey     = t.SalesInvoiceKey;
