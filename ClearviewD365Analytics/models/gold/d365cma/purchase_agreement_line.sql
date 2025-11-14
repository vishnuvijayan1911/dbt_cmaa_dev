{{ config(materialized='view', schema='gold', alias="Purchase agreement line") }}

SELECT  t.PurchaseAgreementLineKey        AS [Purchase agreement line key]
    , NULLIF(t.PurchaseAgreementID, '')   AS [Purchase agreement #]
    , NULLIF(t.LineNumber, '')            AS [Line]
    , NULLIF(t.AgreementClassification,'')AS [Agreement classification]
    , NULLIF(t.DocumentTitle,'')          AS [Document title]
    , NULLIF(du.UOM, '')                  AS [Agreement UOM]
    , NULLIF(dd.Date, '1/1/1900')         AS [Effective date]
    , NULLIF(dd1.Date, '1/1/1900')        AS [Expiration date]
  FROM {{ ref("PurchaseAgreementLine") }}           t 
  LEFT JOIN {{ ref("PurchaseAgreementLine_Fact") }} f 
    ON f.PurchaseAgreementLineKey = t.PurchaseAgreementLineKey
  LEFT JOIN {{ ref("Date") }}                    dd 
    ON dd.DateKey              = f.EffectiveDateKey
  LEFT JOIN {{ ref("Date") }}                    dd1 
    ON dd1.DateKey             = f.ExpirationDateKey
  LEFT JOIN {{ ref("UOM") }}                     du 
    ON du.UOMKey               = f.AgreementUOMKey;
