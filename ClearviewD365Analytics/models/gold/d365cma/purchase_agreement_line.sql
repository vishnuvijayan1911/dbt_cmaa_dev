{{ config(materialized='view', schema='gold', alias="Purchase agreement line") }}

SELECT  t.PurchaseAgreementLineKey        AS [Purchase agreement line key]
    , NULLIF(t.PurchaseAgreementID, '')   AS [Purchase agreement #]
    , NULLIF(t.LineNumber, '')            AS [Line]
    , NULLIF(t.AgreementClassification,'')AS [Agreement classification]
    , NULLIF(t.DocumentTitle,'')          AS [Document title]
    , NULLIF(du.UOM, '')                  AS [Agreement UOM]
    , NULLIF(dd.Date, '1/1/1900')         AS [Effective date]
    , NULLIF(dd1.Date, '1/1/1900')        AS [Expiration date]
  FROM {{ ref("purchaseagreementline_d") }}           t 
  LEFT JOIN {{ ref("purchaseagreementline_f") }} f 
    ON f.PurchaseAgreementLineKey = t.PurchaseAgreementLineKey
  LEFT JOIN {{ ref('date_d') }}                    dd 
    ON dd.DateKey              = f.EffectiveDateKey
  LEFT JOIN {{ ref('date_d') }}                    dd1 
    ON dd1.DateKey             = f.ExpirationDateKey
  LEFT JOIN {{ ref("uom_d") }}                     du 
    ON du.UOMKey               = f.AgreementUOMKey;
