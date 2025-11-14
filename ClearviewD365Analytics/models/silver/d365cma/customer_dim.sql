{{ config(materialized='table', tags=['silver']) }}

-- Source file: cma/cma/layers/_base/_silver/customer/customer.py
-- Root method: Customer.customerdetail [CustomerDetail]
-- Inlined methods: Customer.customerdetail1 [CustomerDetail1], Customer.customerelectronicaddress [CustomerElectronicAddress]
-- external_table_name: CustomerDetail
-- schema_name: temp

WITH
customerdetail1 AS (
    SELECT *

          FROM (   SELECT ct.dataareaid                                                  AS LegalEntityID
                        , ct.accountnum                                                  AS CustomerAccount
                        , dpt.name + ' ' + '-' + ' '+ ct.accountnum                 AS Customer
                        , dpt.name                                                       AS CustomerName
                        , dpt.namealias                                                  AS CustomerAlias
                        , CASE WHEN ct.blocked = 0
                               THEN 'No hold'
                               WHEN ct.blocked = 1
                               THEN 'Hold - Invoice'
                               WHEN ct.blocked = 2
                               THEN 'Hold - All'
                               WHEN ct.blocked = 3
                               THEN 'Hold - Payment'
                               WHEN ct.blocked = 4
                               THEN 'Hold - Requisition'
                               WHEN ct.blocked = 5
                               THEN 'Hold - Never' END                                   AS OnHoldStatus
                        , ct.invoiceaccount                                              AS InvoiceAccount
                        , CASE WHEN ct.blocked <> 0 THEN 1 ELSE 0 END                    AS IsOnHold
                        , ct.vendaccount                                                 AS ICVendorAccount
                        , ct.lineofbusinessid                                            AS LineOfBusinessID
                        , COALESCE(NULLIF(lob.description, ''), ct.lineofbusinessid, '') AS LineOfBusiness
                        , ct.ouraccountnum                                               AS OurAccountID
                        , sg.description                                                 AS SalesDistrict
                        , ct.salespoolid                                                 AS SalesPoolID
                        , COALESCE(NULLIF(sl.name, ''), ct.salespoolid, '')              AS SalesPool
                        , ct.vatnum                                                      AS VATNumber
                        , dpt.recid                                                      AS RecID_DPT
                        , ct.recid                                                       AS _RecID
                        , 1                                                              AS _SourceID
                        , ROW_NUMBER() OVER (PARTITION BY ct.dataareaid, ct.accountnum
    ORDER BY ct.recid   )                                                                AS RankVal
                     FROM {{ ref('custtable') }}                      AS ct
                     LEFT JOIN {{ ref('dirpartytable') }}               dpt
                       ON dpt.recid            = ct.party
                     LEFT JOIN {{ ref('lineofbusiness') }}              lob
                       ON lob.dataareaid       = ct.dataareaid
                      AND lob.lineofbusinessid = ct.lineofbusinessid
                     LEFT JOIN {{ ref('salespool') }}                   sl
                       ON sl.dataareaid        = ct.dataareaid
                      AND sl.salespoolid       = ct.salespoolid
                     LEFT JOIN {{ ref('smmbusrelsalesdistrictgroup') }} sg
                       ON sg.dataareaid        = ct.dataareaid
                      AND sg.salesdistrictid   = ct.salesdistrictid
                    WHERE ct.accountnum <> '') AS t
         WHERE t.RankVal = 1;
),
customerelectronicaddress AS (
    SELECT t.Partition
             , t.Party
             , t.Type
             , t.Locator

          FROM (   SELECT dpl.partition AS Partition
                        , dpl.party     AS Party
                        , lea.type      AS Type
                        , lea.locator   AS Locator
                        , ROW_NUMBER() OVER (PARTITION BY dpl.party, lea.type
    ORDER BY lea.isprimary DESC)        AS RankVal
                     FROM {{ ref('custtable') }}                      ct
                    INNER JOIN {{ ref('dirpartytable') }}             dpt
                       ON dpt.recid    = ct.party
                    INNER JOIN {{ ref('dirpartylocation') }}           dpl
                       ON dpl.party    = dpt.recid
                    INNER JOIN {{ ref('logisticselectronicaddress') }} lea
                       ON dpl.location = lea.location
                      AND lea.type IN ( 1, 2 ) 
          ) AS t
         WHERE t.RankVal = 1;
)
SELECT ROW_NUMBER() OVER (ORDER BY ts._RecID, ts._SourceID) AS CustomerKey  
         , ts.LegalEntityID                                                                     AS LegalEntityID
         , ts.CustomerAccount                                                                   AS CustomerAccount
         , ts.Customer                                                                          AS Customer
         , CASE WHEN ts.CustomerName = '' THEN ts.Customer ELSE ts.CustomerName END             AS CustomerName
         , ts.CustomerAlias                                                                     AS CustomerAlias
         , ts.OnHoldStatus                                                                      AS OnHoldStatus
         , ts.InvoiceAccount                                                                    AS InvoiceAccount
         , ts.IsOnHold                                                                          AS IsOnHold
         , te2.Locator                                                                          AS CustomerEMail
         , te1.Locator                                                                          AS CustomerPhone
         , ts.ICVendorAccount                                                                   AS ICVendorAccount
         , ts.LineOfBusinessID                                                                  AS LineOfBusinessID
         , CASE WHEN ts.LineOfBusiness = '' THEN ts.LineOfBusinessID ELSE ts.LineOfBusiness END AS LineOfBusiness
         , ts.OurAccountID                                                                      AS OurAccountID
         , ts.SalesDistrict                                                                     AS SalesDistrict
         , ts.SalesPoolID                                                                       AS SalesPoolID
         , CASE WHEN ts.SalesPool = '' THEN ts.SalesPoolID ELSE ts.SalesPool END                AS SalesPool
         , ts.VATNumber                                                                         AS VATNumber
         , ts._RecID                                                                            AS _RecID
         , ts._SourceID                                                                         AS _SourceID
         , CURRENT_TIMESTAMP                                                                    AS _ModifiedDate
         ,'1900-01-01'                                                                          AS ActivityDate
      FROM customerdetail1                ts
      LEFT JOIN customerelectronicaddress te1
        ON te1.Party = ts.RecID_DPT
       AND te1.Type  = 1 
      LEFT JOIN customerelectronicaddress te2
        ON te2.Party = ts.RecID_DPT
       AND te2.Type  = 2 
     WHERE ts.CustomerAccount <> ''
