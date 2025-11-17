{{ config(materialized='view', schema='gold', alias="Sales invoice line charge fact") }}

SELECT  t.SalesInvoiceLineChargeKey  AS [Sales invoice line charge key]
  , t.SalesInvoiceLineKey        AS [Sales invoice line key]
  , t.ChargeCategoryKey          AS [Charge category key]
  , t.ChargeCodeKey              AS [Charge code key]
  , t.ChargeCurrencyKey          AS [Charge currency key]
  , t.ChargeTypeKey              AS [Charge type key]
  , t.LegalEntityKey             AS [Legal entity key]
  , t.PricingUOMKey              AS [Price UOM key]
  , t.TransDateKey               AS [Trans date key]
  , t.VoucherKey                 AS [Voucher key]
  , t.AdditionalCharge           AS [Additional charges]
  , t.AdditionalCharge_TransCur  AS [Additional charges in trans currency]
  , t.IncludedCharge             AS [Included charges]
  , t.IncludedCharge_TransCur    AS [Included charges in trans currency]
  , t.IncludeInTotalPrice        AS [Include in total price]
  , t.NonBillableCharge          AS [Non-billable charges]
  , t.NonBillableCharge_TransCur AS [Non-billable charges in trans currency]
  , t.TaxAmount                  AS [Tax]
  , t.TaxAmount_TransCur         AS [Tax in trans currency]
  , t.TotalCharges               AS [Total charges]
  , t.TotalCharges_TransCur      AS [Total charges in trans currency]
FROM {{ ref("salesinvoicelinecharge_fact") }} t 
LEFT JOIN {{ ref("salesinvoiceline_fact") }}  silf 
  ON silf.SalesInvoiceLineKey = t.SalesInvoiceLineKey
LEFT JOIN {{ ref("salesinvoice") }}           si 
  ON si.SalesInvoiceKey       = silf.SalesInvoiceKey
