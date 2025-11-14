{{ config(materialized='view', schema='gold', alias="Vendor") }}

SELECT  t.VendorKey                                                       AS [Vendor key]
  , f.LegalEntityKey                                                  AS [Legal entity key]
  , dd.Date                                                           AS [Vendor create date]
  , NULLIF(dm.DeliveryModeID, '')                                     AS [Vendor delivery mode]
  , NULLIF(dm.DeliveryMode, '')                                       AS [Vendor delivery mode name]
  , NULLIF(dt.DeliveryTermID, '')                                     AS [Vendor delivery term]
  , NULLIF(dt.DeliveryTerm, '')                                       AS [Vendor delivery term name]
  , NULLIF(t.OurAccountID, '')                                        AS [Our account #]
  , NULLIF(pm.PaymentModeID, '')                                      AS [Vendor payment mode]
  , NULLIF(pm.PaymentMode, '')                                        AS [Vendor payment mode name]
  , NULLIF(pt.PaymentTermID, '')                                      AS [Vendor payment term]
  , NULLIF(pt.PaymentTerm, '')                                        AS [Vendor payment term name]
  , NULLIF(t.Vendor, '')                                              AS [Vendor]
  , CASE WHEN t.VendorKey <> -1 THEN CAST(1 AS SMALLINT)ELSE NULL END AS [Vendors]
  , NULLIF(t.VendorAccount, '')                                       AS [Vendor #]
  , NULLIF(t.VendorAlias, '')                                         AS [Vendor alias]
  , NULLIF(a.City, '')                                                AS [Vendor city]
  , NULLIF(a.CountryID, '')                                           AS [Vendor country]
  , NULLIF(a.Country, '')                                             AS [Vendor country name]
  , NULLIF(t.VendorEMail, '')                                         AS [Vendor e-mail]
  , ISNULL(NULLIF(t.VendorGroupID, ''), 'Other')                      AS [Vendor group]
  , ISNULL(NULLIF(t.VendorGroup, ''), 'Other')                        AS [Vendor group name]
  , CASE WHEN t.VendorGroup LIKE 'MRO' THEN 'MRO' ELSE 'Non-MRO' END  AS [Vendor type]
  , NULLIF(t.InvoiceAccount, '')                                      AS [Vendor invoice account]
  , NULLIF(LTRIM(t.VendorName), '')                                   AS [Vendor name]
  , NULLIF(t.OnHoldStatus, '')                                        AS [Vendor on-hold status]
  , NULLIF(t.VendorPhone, '')                                         AS [Vendor phone]
  , NULLIF(a.StateProvince, '')                                       AS [Vendor state province]
  , NULLIF(a.Street, '')                                              AS [Vendor street]
  , NULLIF(a.PostalCode, '')                                          AS [Vendor postal code]
  , NULLIF(t.VATNumber, '')                                           AS [Vendor VAT number]
  , CAST(1 AS INT)                                                    AS [Vendor count]
FROM {{ ref("Vendor") }}              t 
LEFT JOIN {{ ref("Vendor_Fact") }}    f 
  ON f.VendorKey           = t.VendorKey
LEFT JOIN {{ ref("Address") }}        a 
  ON a.AddressKey          = f.AddressKey
LEFT JOIN {{ ref("LegalEntity") }}    l 
  ON l.LegalEntityKey      = f.LegalEntityKey
LEFT JOIN {{ ref("PaymentTerm") }}    pt 
  ON pt.PaymentTermKey     = f.PaymentTermKey
LEFT JOIN {{ ref("DeliveryTerm") }}   dt 
  ON dt.DeliveryTermKey    = f.DeliveryTermKey
LEFT JOIN {{ ref("PaymentMode") }}    pm 
  ON pm.PaymentModeKey     = f.PaymentModeKey
LEFT JOIN {{ ref("DeliveryMode") }}   dm 
  ON dm.DeliveryModeKey    = f.DeliveryModeKey
LEFT JOIN {{ ref("Date") }}           dd
  ON dd.DateKey            = f.CreatedDateKey;
