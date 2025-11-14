{{ config(materialized='table', tags=['silver']) }}

-- Source file: cma/cma/layers/_base/_silver/caseassociation_fact/caseassociation_fact.py
-- Root method: CaseassociationFact.caseassociation_factdetail [CaseAssociation_FactDetail]
-- Inlined methods: CaseassociationFact.caseassociation_factstage [CaseAssociation_FactStage], CaseassociationFact.caseassociation_factitem [CaseAssociation_FactItem], CaseassociationFact.caseassociation_factproduct [CaseAssociation_FactProduct]
-- external_table_name: CaseAssociation_FactDetail
-- schema_name: temp

WITH
caseassociation_factstage AS (
    SELECT ca.isprimary                                                                  AS IsPrimary
         , ca.entitytype                                                                 AS EntityTypeID
         , cdb.caseid                                                                    AS CaseID
         , cdb.recid                                                                     AS RecID_CDB
         , vit.recid                                                                     AS RecID_VIT
         , ib.inventbatchid                                                              AS TagID
         , ib.itemid                                                                     AS TagItemID
         , CASE WHEN ca.entitytype = 8 THEN st.salesid ELSE '' END                       AS SalesOrderID
         , CASE WHEN ca.entitytype = 4 THEN ct.accountnum ELSE '' END                    AS CustomerAccount
         , CASE WHEN ca.entitytype = 21 AND st.salestype = 4 THEN st.recid ELSE NULL END AS RecID_ReturnItem
         , CASE WHEN ca.entitytype = 12 THEN cij.invoiceid ELSE '' END                   AS SalesInvoiceID
         , CASE WHEN ca.entitytype = 22 THEN cdb1.caseid ELSE '' END                     AS RefCaseID
         , CASE WHEN ca.entitytype = 1 THEN hw.personnelnumber ELSE '' END               AS WorkerID
         , CASE WHEN ca.entitytype = 5 THEN vt.accountnum ELSE '' END                    AS VendorAccount
         , CASE WHEN ca.entitytype = 9 THEN pt.purchid ELSE '' END                       AS PurchaseOrderID
         , CASE WHEN ca.entitytype = 18 THEN vij.invoiceid ELSE '' END                   AS PurchaseInvoiceID
         , CASE WHEN ca.entitytype = 20 THEN pl.purchid ELSE '' END                      AS PurchaseOrderLineID
         , CASE WHEN ca.entitytype = 26 THEN pt1.prodid ELSE '' END                      AS ProductionOrderID
         , CASE WHEN ca.entitytype = 13 THEN cit.invoiceid ELSE '' END                   AS SalesInvoiceLineID
         , CASE WHEN ca.entitytype = 24 THEN bt.bomid ELSE '' END                        AS BOMID
         , CASE WHEN ca.entitytype = 14 THEN dct.voucher ELSE '' END                     AS CustomerTransactionID
         , CASE WHEN ca.entitytype = 25 THEN rt.routeid ELSE '' END                      AS RouteID
         , CASE WHEN ca.entitytype = 27 THEN qt.qualityorderid ELSE '' END               AS QualityOrderID
         , cdb.dataareaid                                                                AS LegalEntityID
         , ca.recid                                                                      AS _RecID
         , 1                                                                             AS _SourceID
      FROM {{ ref('caseassociation') }}              ca
      LEFT JOIN {{ ref('casedetailbase') }}          cdb
        ON cdb.recid      = ca.caserecid
      LEFT JOIN {{ ref('salestable') }}              st
        ON st.recid       = ca.refrecid
      LEFT JOIN {{ ref('custtable') }}               ct
        ON ct.recid       = ca.refrecid
      LEFT JOIN {{ ref('dirpartytable') }}           dpt
        ON dpt.recid      = ct.party
      LEFT JOIN {{ ref('custinvoicejour') }}         cij
        ON cij.recid      = ca.refrecid
      LEFT JOIN {{ ref('custtable') }}               ct1
        ON ct1.accountnum = cij.orderaccount
      LEFT JOIN {{ ref('dirpartytable') }}           dpt1
        ON dpt1.recid     = ct1.party
      LEFT JOIN {{ ref('inventbatch') }}             ib
        ON ib.recid       = ca.refrecid
       AND ca.entitytype  = 15
      LEFT JOIN {{ ref('casedetailbase') }}          cdb1
        ON cdb1.recid     = ca.refrecid
      LEFT JOIN {{ ref('hcmworker') }}               hw
        ON hw.recid       = ca.refrecid
      LEFT JOIN {{ ref('vendtable') }}               vt
        ON vt.recid       = ca.refrecid
      LEFT JOIN {{ ref('purchtable') }}              pt
        ON pt.recid       = ca.refrecid
      LEFT JOIN {{ ref('custtrans') }}               dct
        ON dct.recid      = ca.refrecid
      LEFT JOIN {{ ref('vendinvoicejour') }}         vij
        ON vij.recid      = ca.refrecid
      LEFT JOIN {{ ref('purchline') }}               pl
        ON pl.recid       = ca.refrecid
      LEFT JOIN {{ ref('prodtable') }}               pt1
        ON pt1.recid      = ca.refrecid
      LEFT JOIN {{ ref('custinvoicetrans') }}        cit
        ON cit.recid      = ca.refrecid
      LEFT JOIN {{ ref('routetable') }}              rt
        ON rt.recid       = ca.refrecid
      LEFT JOIN {{ ref('bomtable') }}                bt
        ON bt.recid       = ca.refrecid
      LEFT JOIN {{ ref('vendinvoicetrans') }}        vit
        ON vit.recid      = ca.refrecid
       AND ca.entitytype  = 19
      LEFT JOIN {{ ref('inventqualityordertable') }} qt
        ON qt.recid       = ca.refrecid;
),
caseassociation_factitem AS (
    SELECT *
      FROM (   SELECT it.itemid                     AS ItemID
                    , it.dataareaid                 AS LegalEntityID
                    , ISNULL (id.configid, '')      AS ProductConfig
                    , ISNULL (id.inventsizeid, '')  AS ProductWidth
                    , ISNULL (id.inventcolorid, '') AS ProductLength
                    , ISNULL (id.inventstyleid, '') AS ProductColor
                    , ca.recid                      AS RecID_CA
                    , ROW_NUMBER () OVER (PARTITION BY ca.recid
    ORDER BY it.recid, id.recid DESC)               AS RankVal
                 FROM {{ ref('caseassociation') }}               ca
                INNER JOIN {{ ref('inventtable') }}              it
                   ON it.recid                  = ca.refrecid
                INNER JOIN {{ ref('ecoresdistinctproductvariant') }}    pr -- Products
                   ON pr.recid                  = it.product
                   OR pr.productmaster          = it.product
                 LEFT JOIN {{ ref('ecoresproducttranslation') }} pt
                   ON pt.product                = pr.recid
                  AND pt.languageid             = 'en-us'
                 LEFT JOIN {{ ref('inventdimcombination') }}     ic
                   ON ic.dataareaid             = it.dataareaid
                  AND ic.distinctproductvariant = pr.recid
                 LEFT JOIN {{ ref('inventdim') }}                id
                   ON id.dataareaid             = ic.dataareaid
                  AND id.inventdimid            = ic.inventdimid
                WHERE ca.entitytype = 11) t
     WHERE t.RankVal = 1;
),
caseassociation_factproduct AS (
    SELECT *
      FROM (   SELECT it.itemid                     AS ItemID
                    , it.dataareaid                 AS LegalEntityID
                    , ISNULL (id.configid, '')      AS ProductConfig
                    , ISNULL (id.inventsizeid, '')  AS ProductWidth
                    , ISNULL (id.inventcolorid, '') AS ProductLength
                    , ISNULL (id.inventstyleid, '') AS ProductColor
                    , ca.recid                      AS RecID_CA
                    , ROW_NUMBER () OVER (PARTITION BY ca.recid
    ORDER BY it.recid, id.recid DESC)               AS RankVal
                 FROM {{ ref('caseassociation') }}                   ca
                INNER JOIN {{ ref('casedetailbase') }}               cdb
                   ON ca.caserecid              = cdb.recid
                INNER JOIN {{ ref('ecoresproduct') }}                epr
                   ON epr.recid                 = ca.refrecid
                INNER JOIN {{ ref('inventtable') }}                  it
                   ON it.dataareaid             = cdb.dataareaid
                  AND it.product                = epr.recid
                INNER JOIN {{ ref('ecoresdistinctproductvariant') }} pr -- Products
                   ON pr.recid                  = it.product
                   OR pr.productmaster          = it.product
                 LEFT JOIN {{ ref('ecoresproducttranslation') }}     pt
                   ON pt.product                = pr.recid
                  AND pt.languageid             = 'en-us'
                 LEFT JOIN {{ ref('inventdimcombination') }}         ic
                   ON ic.dataareaid             = it.dataareaid
                  AND ic.distinctproductvariant = pr.recid
                 LEFT JOIN {{ ref('inventdim') }}                    id
                   ON id.dataareaid             = ic.dataareaid
                  AND id.inventdimid            = ic.inventdimid
                WHERE ca.entitytype = 23) t
     WHERE t.RankVal = 1;
)
SELECT ROW_NUMBER () OVER (ORDER BY ts._RecID, ts._SourceID) AS CaseAssociationKey
     , ISNULL (cd.CaseKey, -1)                               AS CaseKey
     , ISNULL (db.BOMKey, -1)                                AS EntityBOMKey
     , ISNULL (c.CustomerKey, -1)                            AS EntityCustomerKey
     , ISNULL (de.EmployeeKey, -1)                           AS EntityEmployeeKey
     , ISNULL (dp.ProductKey, -1)                            AS EntityProductKey
     , ISNULL (dp1.ProductKey, -1)                           AS EntityProductItemKey
     , ISNULL (dpi.PurchaseInvoiceKey, -1)                   AS EntityPurchaseInvoiceKey
     , ISNULL (pil.PurchaseInvoiceLineKey, -1)               AS EntityPurchaseInvoiceLineKey
     , ISNULL (po.PurchaseOrderKey, -1)                      AS EntityPurchaseOrderKey
     , ISNULL (pol.PurchaseOrderLineKey, -1)                 AS EntityPurchaseOrderLineKey
     , ISNULL (p.ProductionKey, -1)                          AS EntityProductionKey
     , ISNULL (cd1.CaseKey, -1)                              AS EntityCaseKey
     , ISNULL (so1.SalesOrderKey, -1)                        AS EntityReturnOrderKey
     , ISNULL (sil.SalesInvoiceLineKey, -1)                  AS EntitySalesInvoiceLineKey
     , ISNULL (so.SalesOrderKey, -1)                         AS EntitySalesOrderKey
     , ISNULL (si.SalesInvoiceKey, -1)                       AS EntitySalesInvoiceKey
     , ISNULL (dt.TagKey, -1)                                AS EntityTagKey
     , ISNULL (v.VendorKey, -1)                              AS EntityVendorKey
     , ISNULL (qo.QualityOrderKey, -1)                       AS EntityQualityOrderKey
     , ISNULL (le.LegalEntityKey, -1)                        AS LegalEntityKey
     , we.localizedlabel                                     AS EntityType
     , CASE WHEN we.localizedlabel = 'BOM/Formula'
            THEN db.BOMID
            WHEN we.localizedlabel = 'Customer'
            THEN c.CustomerAccount
            WHEN we.localizedlabel = 'Worker'
            THEN de.PersonnelNumber
            WHEN we.localizedlabel = 'Purchase order'
            THEN po.PurchaseOrderID
            WHEN we.localizedlabel = 'Product'
            THEN dp.ItemID
            WHEN we.localizedlabel = 'Item'
            THEN dp1.ItemID
            WHEN we.localizedlabel = 'Production order'
            THEN p.ProductionID
            WHEN we.localizedlabel = 'Vendor invoice'
            THEN dpi.InvoiceID
            WHEN we.localizedlabel = 'Vendor invoice line'
            THEN pil.InvoiceID
            WHEN we.localizedlabel = 'Purchase order line'
            THEN pol.PurchaseOrderID
            WHEN we.localizedlabel = 'Case'
            THEN cd1.CaseID
            WHEN we.localizedlabel = 'Returned order'
            THEN so1.RMANumber
            WHEN we.localizedlabel = 'Customer invoice'
            THEN si.InvoiceID
            WHEN we.localizedlabel = 'Customer invoice line'
            THEN sil.InvoiceID
            WHEN we.localizedlabel = 'Sales order'
            THEN so.SalesOrderID
            WHEN we.localizedlabel = 'Tags'
            THEN dt.TagID
            WHEN we.localizedlabel = 'Vendor'
            THEN v.VendorAccount
            WHEN we.localizedlabel = 'Quality order'
            THEN qo.QualityOrderID
            ELSE '' END                                      AS EntityID
     , ts.IsPrimary                                          AS IsPrimary
     , ts._RecID                                             AS _RecID
     , ts._SourceID                                          AS _SourceID
     , CURRENT_TIMESTAMP                                     AS _CreatedDate
     , CURRENT_TIMESTAMP                                     AS _ModifiedDate
  FROM caseassociation_factstage        ts
  LEFT JOIN caseassociation_factitem    ti
    ON ti.RecID_CA         = ts._RecID
  LEFT JOIN caseassociation_factproduct tp
    ON tp.RecID_CA         = ts._RecID
  LEFT JOIN {{ ref('globaloptionsetmetadata') }}   we
    ON we.optionsetname    = 'EntityType'
   AND we.[Option]         = ts.EntityTypeID
  LEFT JOIN silver.cma_Case                      cd
    ON cd._RecID           = ts.RecID_CDB
  LEFT JOIN silver.cma_LegalEntity               le
    ON le.LegalEntityID    = ts.LegalEntityID
  LEFT JOIN silver.cma_Customer                  c
    ON c.LegalEntityID     = ts.LegalEntityID
   AND c.CustomerAccount   = ts.CustomerAccount
  LEFT JOIN silver.cma_SalesOrder                so
    ON so.LegalEntityID    = ts.LegalEntityID
   AND so.SalesOrderID     = ts.SalesOrderID
  LEFT JOIN silver.cma_SalesInvoice              si
    ON si.LegalEntityID    = ts.LegalEntityID
   AND si.InvoiceID        = ts.SalesInvoiceID
  LEFT JOIN silver.cma_Tag                       dt
    ON dt.LegalEntityID    = ts.LegalEntityID
   AND dt.TagID            = ts.TagID
   AND dt.ItemID           = ts.TagItemID
   AND ts.EntityTypeID     = 15
  LEFT JOIN silver.cma_Case                      cd1
    ON cd1.CaseID          = ts.RefCaseID
  LEFT JOIN silver.cma_Vendor                    v
    ON v.LegalEntityID     = ts.LegalEntityID
   AND v.VendorAccount     = ts.VendorAccount
  LEFT JOIN silver.cma_PurchaseOrder             po
    ON po.LegalEntityID    = ts.LegalEntityID
   AND po.PurchaseOrderID  = ts.PurchaseOrderID
  LEFT JOIN silver.cma_Product                   dp
    ON dp.ItemID           = tp.ItemID
   AND dp.LegalEntityID    = tp.LegalEntityID
   AND dp.ProductWidth     = tp.ProductWidth
   AND dp.ProductLength    = tp.ProductLength
   AND dp.ProductColor     = tp.ProductColor
   AND dp.ProductConfig    = tp.ProductConfig
  LEFT JOIN silver.cma_Product                   dp1
    ON dp1.ItemID          = ti.ItemID
   AND dp1.LegalEntityID   = ti.LegalEntityID
   AND dp1.ProductWidth    = ti.ProductWidth
   AND dp1.ProductLength   = ti.ProductLength
   AND dp1.ProductColor    = ti.ProductColor
   AND dp1.ProductConfig   = ti.ProductConfig
  LEFT JOIN silver.cma_PurchaseInvoice           dpi
    ON dpi.LegalEntityID   = ts.LegalEntityID
   AND dpi.InvoiceID       = ts.PurchaseInvoiceID
  LEFT JOIN silver.cma_PurchaseOrderLine         pol
    ON pol.LegalEntityID   = ts.LegalEntityID
   AND pol.PurchaseOrderID = ts.PurchaseOrderLineID
  LEFT JOIN silver.cma_Production                p
    ON p.LegalEntityID     = ts.LegalEntityID
   AND p.ProductionID      = ts.ProductionOrderID
  LEFT JOIN silver.cma_SalesInvoiceLine          sil
    ON sil.LegalEntityID   = ts.LegalEntityID
   AND sil.InvoiceID       = ts.SalesInvoiceLineID
  LEFT JOIN silver.cma_BOM                       db
    ON db.LegalEntityID    = ts.LegalEntityID
   AND db.BOMID            = ts.BOMID
  LEFT JOIN silver.cma_Employee                  de
    ON de.PersonnelNumber  = ts.WorkerID
  LEFT JOIN silver.cma_PurchaseInvoiceLine       pil
    ON pil._RecID2         = ts.RecID_VIT
  LEFT JOIN silver.cma_SalesOrder                so1
    ON so1.LegalEntityID   = ts.LegalEntityID
   AND so1._RecID          = ts.RecID_ReturnItem
  LEFT JOIN silver.cma_QualityOrder              qo
    ON qo.LegalEntityID    = ts.LegalEntityID
   AND qo.QualityOrderID   = ts.QualityOrderID;
