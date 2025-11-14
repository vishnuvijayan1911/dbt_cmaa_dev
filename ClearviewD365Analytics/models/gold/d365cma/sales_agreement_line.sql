{{ config(materialized='view', schema='gold', alias="Sales agreement line") }}

SELECT  t.SalesAgreementLineKey        AS [Sales agreement line key]
    , NULLIF(t.SalesAgreementID, '') AS [Sales agreement #]
    , NULLIF(t.LineNumber, '')       AS [Line]
    , NULLIF(du.UOM, '')             AS [Agreement UOM]
   , NULLIF (t.agreementclassification, '') AS [Agreement classification]
   , NULLIF (t.agreementstatus, '')       AS [Agreement status]
   , NULLIF (t.customerpartnumber, '')    AS [Customer part number]
    , NULLIF(dd.Date, '1/1/1900')    AS [Effective date]
    , NULLIF(dd1.Date, '1/1/1900')   AS [Expiration date]
    , NULLIF(dd2.Date, '1/1/1900')   AS [Line effective date]
    , NULLIF(dd3.Date, '1/1/1900')   AS [Line expiration date]
   , NULLIF(du1.UOM, '')                  AS [Pricing UOM]
  FROM {{ ref("SalesAgreementLine") }}           t 
  LEFT JOIN {{ ref("SalesAgreementLine_Fact") }} f 
    ON f.SalesAgreementLineKey = t.SalesAgreementLineKey
  LEFT JOIN {{ ref("Date") }}                    dd 
    ON dd.DateKey              = f.EffectiveDateKey
  LEFT JOIN {{ ref("Date") }}                    dd1 
    ON dd1.DateKey             = f.ExpirationDateKey
  LEFT JOIN {{ ref("UOM") }}                     du 
    ON du.UOMKey               = f.AgreementUOMKey
  LEFT JOIN {{ ref("UOM") }}           du1 
    ON du1.UOMKey       = f.PricingUOMKey
  LEFT JOIN {{ ref("Date") }}                    dd2
    ON dd2.DateKey              = f.LineEffectiveDateKey
  LEFT JOIN {{ ref("Date") }}                    dd3
    ON dd3.DateKey              = f.LineExpirationDateKey
