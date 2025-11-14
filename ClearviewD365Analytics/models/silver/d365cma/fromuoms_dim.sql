{{ config(materialized='table', tags=['silver']) }}

-- Source file: cma/cma/layers/_base/_silver/fromuoms/fromuoms.py
-- Root method: FromUoms.from_uoms_detail [FromUomsDetail]
-- external_table_name: FromUomsDetail
-- schema_name: temp

SELECT
  DATAAREAID   AS legalentityid,
  itemid,
  fromuom,
  uom.uomkey   AS fromuomkey,
  uom.uomclass AS fromuomclass,
  productid,
  productkey,
  productwidth,
  productlength 
FROM
  (
      SELECT DISTINCT
        it.dataareaid,
        k.productid,
        k.productkey,
        k.productwidth,
        k.productlength,
        it.itemid,
        im.unitid AS fromuom 
      FROM
      {{ ref('inventtable') }} it 
        INNER JOIN
            silver.cma_Product k 
            ON k.itemid = it.itemid 
        INNER JOIN
            {{ ref('inventtablemodule') }} im 
            ON im.dataareaid = it.dataareaid 
            AND im.itemid = it.itemid 
            AND im.moduletype = 0 
      WHERE
        it.dataareaid <> 'DAT' 
        AND ISNULL(it.itemid, '') <> '' 
        AND ISNULL(im.unitid, '') <> '' 
      UNION
      SELECT DISTINCT
        t.dataareaid,
        k.productid,
        k.productkey,
        k.productwidth,
        k.productlength,
        t.itemid,
        t.salesunit AS fromuom 
      FROM
        {{ ref('salesline') }} t 
        INNER JOIN
            silver.cma_Product k 
            ON k.itemid = t.itemid 
      WHERE
        ISNULL(t.itemid, '') <> '' 
        AND ISNULL(t.salesunit, '') <> '' 
      UNION
      SELECT DISTINCT
        t.dataareaid,
        k.productid,
        k.productkey,
        k.productwidth,
        k.productlength,
        t.itemid,
        t.salesunit AS fromuom 
      FROM
        {{ ref('custpackingsliptrans') }} t 
        INNER JOIN
            silver.cma_Product k 
            ON k.itemid = t.itemid 
      WHERE
        ISNULL(t.itemid, '') <> '' 
        AND ISNULL(t.salesunit, '') <> '' 
      UNION
      SELECT DISTINCT
        t.dataareaid,
        k.productid,
        k.productkey,
        k.productwidth,
        k.productlength,
        t.itemid,
        t.salesunit AS fromuom 
      FROM
        {{ ref('custinvoicetrans') }} t 
        INNER JOIN
            silver.cma_Product k 
            ON k.itemid = t.itemid 
      WHERE
        ISNULL(t.itemid, '') <> '' 
        AND ISNULL(t.salesunit, '') <> '' 
      UNION
      SELECT DISTINCT
        t.dataareaid,
        k.productid,
        k.productkey,
        k.productwidth,
        k.productlength,
        t.itemid,
        t.purchunit AS fromuom 
      FROM
        {{ ref('purchline') }} t 
        INNER JOIN
            silver.cma_Product k 
            ON k.itemid = t.itemid 
        UNION
        SELECT DISTINCT
            t.dataareaid,
            k.productid,
            k.productkey,
            k.productwidth,
            k.productlength,
            t.itemid,
            t.purchunit AS fromuom 
        FROM
            {{ ref('vendpackingsliptrans') }} t 
            INNER JOIN
              silver.cma_Product k 
              ON k.itemid = t.itemid 
        WHERE
            ISNULL(t.itemid, '') <> '' 
            AND ISNULL(t.purchunit, '') <> '' 
        UNION
        SELECT DISTINCT
            t.dataareaid,
            k.productid,
            k.productkey,
            k.productwidth,
            k.productlength,
            t.itemid,
            t.purchunit AS fromuom 
        FROM
            {{ ref('vendinvoicetrans') }} t 
            INNER JOIN
              silver.cma_Product k 
              ON k.itemid = t.itemid 
        WHERE
            ISNULL(t.itemid, '') <> '' 
            AND ISNULL(t.purchunit, '') <> '' 
        UNION
        SELECT DISTINCT
            s.dataareaid,
            k.productid,
            k.productkey,
            k.productwidth,
            k.productlength,
            s.itemid,
            s.salesunitid AS fromuom 
        FROM
            {{ ref('forecastsales') }} s 
            INNER JOIN
              silver.cma_Product k 
              ON k.itemid = s.itemid 
        WHERE
            ISNULL(s.itemid, '') <> '' 
            AND ISNULL(s.salesunitid, '') <> '' 
        UNION
        SELECT DISTINCT
            s.dataareaid,
            k.productid,
            k.productkey,
            k.productwidth,
            k.productlength,
            vit.itemid,
            s.cmapriceuom AS FromUOM 
        FROM
            {{ ref('markuptrans') }} s 
            INNER JOIN
              {{ ref('sqldictionary') }} sd 
              ON sd.fieldid = 0 
              AND sd.tabid = s.transtableid 
              AND sd.name = 'VENDINVOICETRANS' 
            INNER JOIN
              {{ ref('vendinvoicetrans') }} vit 
              ON vit.recid = s.transrecid 
            INNER JOIN
              silver.cma_Product k 
              ON k.itemid = vit.itemid 
        WHERE
            ISNULL (vit.itemid, '') <> '' 
            AND ISNULL (s.cmapriceuom, '') <> '' 
        UNION
        SELECT DISTINCT
            s.dataareaid,
            k.productid,
            k.productkey,
            k.productwidth,
            k.productlength,
            pl.itemid,
            s.cmapriceuom AS FromUOM 
        FROM
            {{ ref('markuptrans') }} s 
            INNER JOIN
              {{ ref('sqldictionary') }} sd 
              ON sd.fieldid = 0 
              AND sd.tabid = s.transtableid 
              AND sd.name = 'PurchLine' 
            INNER JOIN
              {{ ref('purchline') }} pl 
              ON pl.recid = s.transrecid 
            INNER JOIN
              silver.cma_Product k 
              ON k.itemid = pl.itemid 
        WHERE
            ISNULL (pl.itemid, '') <> '' 
            AND ISNULL (s.cmapriceuom, '') <> '' 
        UNION
        SELECT DISTINCT
            pj.dataareaid,
            k.productid,
            k.productkey,
            k.productwidth,
            k.productlength,
            pj.itemid,
            pj.bomunitid AS fromuom 
        FROM
            {{ ref('prodjournalbom') }} pj 
            INNER JOIN
              silver.cma_Product k 
              ON k.itemid = pj.itemid 
        WHERE
            ISNULL(pj.itemid, '') <> '' 
            AND ISNULL(pj.bomunitid, '') <> '' 
        UNION
        SELECT DISTINCT
            pj.dataareaid,
            k.productid,
            k.productkey,
            k.productwidth,
            k.productlength,
            pj.itemid,
            ct.unitid AS fromuom 
        FROM
            {{ ref('prodjournalprod') }} pj 
            INNER JOIN
              silver.cma_Product k 
              ON k.itemid = pj.itemid 
            INNER JOIN
              {{ ref('cmatagactualstable') }} ct 
              ON ct.dataareaid = pj.dataareaid 
              AND ct.referencenumber = pj.prodid 
        WHERE
            ISNULL(pj.itemid, '') <> '' 
            AND ISNULL(ct.unitid, '') <> '' 
        UNION
        SELECT DISTINCT
            pr.inventdimiddataarea AS DATAAREAID,
            k.productid,
            k.productkey,
            k.productwidth,
            k.productlength,
            pr.itemid,
            u.symbol AS fromuom 
        FROM
            {{ ref('purchreqline') }} pr 
            INNER JOIN
              silver.cma_Product k 
              ON k.itemid = pr.itemid 
            INNER JOIN
              {{ ref('unitofmeasure') }} u 
              ON u.recid = pr.purchunitofmeasure 
        WHERE
            ISNULL(pr.itemid, '') <> '' 
            AND ISNULL(u.symbol, '') <> ''
  )
  AS list 
  INNER JOIN
      silver.cma_UOM uom 
      ON lower(list.fromuom) = lower(uom.uom) 
WHERE
  list.itemid <> ''
