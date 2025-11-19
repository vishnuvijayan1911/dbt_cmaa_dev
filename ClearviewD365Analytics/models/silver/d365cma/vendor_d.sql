{{ config(materialized='table', tags=['silver'], alias='vendor') }}

-- Source file: cma/cma/layers/_base/_silver/vendor/vendor.py
-- Root method: Vendor.get_detail_query [VendorDetail]
-- Inlined methods: Vendor.get_stage_query [VendorStage], Vendor.get_electronic_address_query [VendorElectronicAddress]
-- external_table_name: VendorDetail
-- schema_name: temp

WITH
vendorstage AS (
    SELECT vt.dataareaid                                                   AS LegalEntityID
        , vt.accountnum                                                     AS VendorAccount
        , dpt.name + ' ' + '-' +' ' + vt.accountnum                  AS Vendor
        , dpt.name                                                          AS VendorName
        , dpt.namealias                                                     AS VendorAlias
        , CASE WHEN vt.blocked = 0
               THEN 'No hold'
               WHEN vt.blocked = 1
               THEN 'Hold - Invoice'
               WHEN vt.blocked = 2
               THEN 'Hold - All'
               WHEN vt.blocked = 3
               THEN 'Hold - Payment'
               WHEN vt.blocked = 4
               THEN 'Hold - Requisition'
               WHEN vt.blocked = 5
               THEN 'Hold - Never' END                                      AS OnHoldStatus
        , vt.itembuyergroupid                                               AS BuyerGroupID
        , ibg.description                                                   AS BuyerGroup
        , vt.invoiceaccount                                                 AS InvoiceAccount
        , lob.lineofbusinessid                                              AS LineOfBusinessID
        , lob.description                                                   AS LineOfBusiness
        , vt.youraccountnum                                                 AS OurAccountID
        , vt.purchpoolid                                                    AS PurchasePoolID
        , pl.name                                                           AS PurchasePool
        , vt.vatnum                                                         AS VATNumber
        , vt.vendgroup                                                      AS VendorGroupID
        , vg.name                                                           AS VendorGroup
        , CASE WHEN vt.tax1099reports = 1 THEN 'Reports 1099' ELSE NULL END AS Is1099Reported
        , dpt.recid                                                         AS RecID_DPT
        , vt.recid                                                          AS _RecID
        , 1                                                                 AS _SourceID
     FROM {{ ref('vendtable') }}             vt
    INNER JOIN {{ ref('dirpartytable') }}    dpt
       ON dpt.recid            = vt.party
     LEFT JOIN {{ ref('vendgroup') }}        vg
       ON vg.dataareaid        = vt.dataareaid
      AND vg.vendgroup         = vt.vendgroup
     LEFT JOIN {{ ref('lineofbusiness') }}   lob
       ON lob.dataareaid       = vt.dataareaid
      AND lob.lineofbusinessid = vt.lineofbusinessid
     LEFT JOIN {{ ref('purchpool') }}        pl
       ON pl.dataareaid        = vt.dataareaid
      AND pl.purchpoolid       = vt.purchpoolid
     LEFT JOIN {{ ref('inventbuyergroup') }} ibg
       ON ibg.dataareaid      = vt.dataareaid
      AND ibg.[GROUP]           = vt.itembuyergroupid;
),
vendorelectronicaddress AS (
    SELECT t.Partition
            ,t.Party
            ,t.Type
            ,t.Locator
     FROM (   SELECT dpl.partition AS Partition
                   , dpl.party     AS Party
                   , lea.type      AS Type
                   , lea.locator   AS Locator
                   , ROW_NUMBER() OVER (PARTITION BY dpl.party, lea.type
           ORDER BY lea.isprimary DESC)        AS RankVal
                FROM {{ ref('vendtable') }}                       vt
               INNER JOIN  {{ ref('dirpartytable') }}               dpt
                  ON dpt.recid    = vt.party
               INNER JOIN {{ ref('dirpartylocation') }}           dpl
                  ON dpl.party    = dpt.recid
               INNER JOIN {{ ref('logisticselectronicaddress') }} lea
                  ON dpl.location = lea.location
                 AND lea.type IN ( 1, 2 ) -- 1:Phone No, 2:Email
     ) AS t
    WHERE t.RankVal = 1;
)
SELECT ROW_NUMBER() OVER (ORDER BY ts._RecID, ts._SourceID) AS VendorKey 
      , ts.LegalEntityID                                       AS LegalEntityID
      , ts.BuyerGroupID                                        AS BuyerGroupID
      , ts.BuyerGroup                                          AS BuyerGroup
      , ts.InvoiceAccount                                      AS InvoiceAccount
      , ts.Is1099Reported                                      AS Is1099Reported
      , ts.LineOfBusinessID                                    AS LineOfBusinessID
      , ts.LineOfBusiness                                      AS LineOfBusiness
      , ts.OnHoldStatus                                        AS OnHoldStatus
      , ts.OurAccountID                                        AS OurAccountID
      , ts.PurchasePoolID                                      AS PurchasePoolID
      , ts.PurchasePool                                        AS PurchasePool
      , ts.VATNumber                                           AS VATNumber
      , ts.VendorAccount                                       AS VendorAccount
      , ts.Vendor                                              AS Vendor
      , ts.VendorName                                          AS VendorName
      , ts.VendorAlias                                         AS VendorAlias
      , te2.Locator                                            AS VendorEMail
      , ts.VendorGroupID                                       AS VendorGroupID
      , ts.VendorGroup                                         AS VendorGroup
      , te1.Locator                                            AS VendorPhone
      , ts._RecID                                              AS _RecID
      , ts._SourceID                                           AS _SourceID
 FROM vendorstage                  ts
 LEFT JOIN vendorelectronicaddress te1
   ON te1.Party      = ts.RecID_DPT
  AND te1.Type       = 1 -- Phone
 LEFT JOIN vendorelectronicaddress te2
   ON te2.Party      = ts.RecID_DPT
  AND te2.Type       = 2 -- EMail
WHERE ts.VendorAccount <> '';

