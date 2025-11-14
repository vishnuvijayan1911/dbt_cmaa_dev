{{ config(materialized='table', tags=['silver']) }}

-- Source file: cma/cma/layers/_base/_silver/uomfrominvoicecharges/uomfrominvoicecharges.py
-- Root method: UomFromInvoiceCharges.uoms_from_invoice_charges_detail [UomFromInvoiceChargesDetail]
-- external_table_name: UomFromInvoiceChargesDetail
-- schema_name: temp

SELECT
   DATAAREAID       AS legalentityid,
   itemid,
   fromuom,
   fromuom.uomkey   AS fromuomkey,
   fromuom.uomclass AS fromuomclass,
   touom.uomkey     AS touomkey,
   touom.uomclass   AS touomclass,
   touom,
   productid,
   productkey,
   productwidth,
   productlength 
FROM
   (
      SELECT DISTINCT
         mt.dataareaid,
         t.itemid,
         t.purchunit AS fromuom,
         mt.cmapriceuom AS touom,
         k.productid,
         k.productkey,
         k.productwidth,
         k.productlength 
      FROM
         {{ ref('markuptrans') }} mt 
         INNER JOIN
            {{ ref('sqldictionary') }} sd 
            ON sd.fieldid = 0 
            AND sd.tabid = mt.transtableid 
            AND sd.name = 'VendInvoiceTrans' 
         INNER JOIN
            {{ ref('vendinvoicetrans') }} t 
            ON t.recid = mt.transrecid 
         INNER JOIN
            silver.cma_Product k 
            ON k.itemid = t.itemid 
      WHERE
         ISNULL(mt.cmapriceuom, '') <> '' 
         AND ISNULL(t.purchunit, '') <> '' 
         AND ISNULL(t.itemid, '') <> '' 
      UNION
      SELECT DISTINCT
         mt.dataareaid,
         t.itemid,
         t.salesunit AS fromuom,
         mt.cmapriceuom AS touom,
         k.productid,
         k.productkey,
         k.productwidth,
         k.productlength 
      FROM
         {{ ref('markuptrans') }} mt 
         INNER JOIN
            {{ ref('sqldictionary') }} sd 
            ON sd.fieldid = 0 
            AND sd.tabid = mt.transtableid 
            AND sd.name = 'CustInvoiceTrans' 
         INNER JOIN
            {{ ref('custinvoicetrans') }} t 
            ON t.recid = mt.transrecid 
         INNER JOIN
            silver.cma_Product k 
            ON k.itemid = t.itemid 
      WHERE
         ISNULL(mt.cmapriceuom, '') <> '' 
         AND ISNULL(t.salesunit, '') <> '' 
         AND ISNULL(t.itemid, '') <> '' 
      UNION
      SELECT DISTINCT
         mt.dataareaid,
         t.itemid,
         t.purchunit AS fromuom,
         mt.cmapriceuom AS touom,
         k.productid,
         k.productkey,
         k.productwidth,
         k.productlength 
      FROM
         {{ ref('markuptrans') }} mt 
         INNER JOIN
            {{ ref('sqldictionary') }} sd 
            ON sd.fieldid = 0 
            AND sd.tabid = mt.transtableid 
            AND sd.name = 'PurchLine' 
         INNER JOIN
            {{ ref('purchline') }} t 
            ON t.recid = mt.transrecid 
         INNER JOIN
            silver.cma_Product k 
            ON k.itemid = t.itemid 
      WHERE
         ISNULL(mt.cmapriceuom, '') <> '' 
         AND ISNULL(t.purchunit, '') <> '' 
         AND ISNULL(t.itemid, '') <> '' 
      UNION
      SELECT DISTINCT
         t.dataareaid,
         k.itemid,
         itm.unitid AS FromUOM,
         t.purchunit AS touom,
         k.productid,
         k.productkey,
         k.productwidth,
         k.productlength 
      FROM
         {{ ref('purchline') }} t 
         INNER JOIN
            silver.cma_Product k 
            ON k.itemid = t.itemid 
         INNER JOIN
            {{ ref('inventtablemodule') }} itm 
            ON itm.dataareaid = k.LegalEntityID 
            AND itm.itemid = k.itemId 
            AND itm.moduletype = 2 
         UNION
         SELECT DISTINCT
            mt.dataareaid,
            t.itemid,
            t.salesunit AS fromuom,
            mt.cmapriceuom AS touom,
            k.productid,
            k.productkey,
            k.productwidth,
            k.productlength 
         FROM
            {{ ref('markuptrans') }} mt 
            INNER JOIN
               {{ ref('sqldictionary') }} sd 
               ON sd.fieldid = 0 
               AND sd.tabid = mt.transtableid 
               AND sd.name = 'SalesLine' 
            INNER JOIN
               {{ ref('salesline') }} t 
               ON t.recid = mt.transrecid 
            INNER JOIN
               silver.cma_Product k 
               ON k.itemid = t.itemid 
         WHERE
            ISNULL(mt.cmapriceuom, '') <> '' 
            AND ISNULL(t.salesunit, '') <> '' 
            AND ISNULL(t.itemid, '') <> ''
   )
   list 
   INNER JOIN
      silver.cma_UOM fromuom 
      ON lower(fromuom.uom) = lower(list.fromuom) 
   INNER JOIN
      silver.cma_UOM touom 
      ON lower(touom.uom) = lower(list.touom)
