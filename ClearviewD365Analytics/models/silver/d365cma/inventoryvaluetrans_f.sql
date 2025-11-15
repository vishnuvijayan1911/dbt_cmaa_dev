{{ config(materialized='table', tags=['silver'], alias='inventoryvaluetrans_fact') }}

-- Source file: cma/cma/layers/_base/_silver/inventoryvaluetrans_f/inventoryvaluetrans_f.py
-- Root method: InventoryvaluetransFact.inventoryvaluetrans_factdetail [InventoryValueTrans_FactDetail]
-- Inlined methods: InventoryvaluetransFact.inventoryvaluetrans_factinventvaluephysicaladjustment [InventoryValueTrans_FactINVENTVALUEPHYSICALADJUSTMENT], InventoryvaluetransFact.inventoryvaluetrans_factinventvaluephysicaltransaction [InventoryValueTrans_FactINVENTVALUEPHYSICALTRANSACTION], InventoryvaluetransFact.inventoryvaluetrans_factinventvaluefinancialtransaction [InventoryValueTrans_FactINVENTVALUEFINANCIALTRANSACTION], InventoryvaluetransFact.inventoryvaluetrans_factinventvaluephysicalreversed [InventoryValueTrans_FactINVENTVALUEPHYSICALREVERSED], InventoryvaluetransFact.inventoryvaluetrans_factinventvaluephysicalandfinancialsettlement [InventoryValueTrans_FactINVENTVALUEPHYSICALANDFINANCIALSETTLEMENT], InventoryvaluetransFact.inventoryvaluetrans_factinventvaluetransunionall [InventoryValueTrans_FactINVENTVALUETRANSUNIONALL], InventoryvaluetransFact.inventoryvaluetrans_factinventdim [InventoryValueTrans_FactInventDim], InventoryvaluetransFact.inventoryvaluetrans_factinventtrans [InventoryValueTrans_FactInventTrans], InventoryvaluetransFact.inventoryvaluetrans_factstage [InventoryValueTrans_FactStage], InventoryvaluetransFact.inventoryvaluetrans_factfinancial [InventoryValueTrans_FactFinancial], InventoryvaluetransFact.inventoryvaluetrans_factdetail1 [InventoryValueTrans_FactDetail1], InventoryvaluetransFact.inventoryvaluetrans_factuomconversion [InventoryValueTrans_FactUOMConversion]
-- external_table_name: InventoryValueTrans_FactDetail
-- schema_name: temp

WITH
inventoryvaluetrans_factinventvaluephysicaladjustment AS (
    SELECT it.inventdimid                                            AS INVENTDIMID
             , it.statusissue                                            AS STATUSISSUE
             , it.statusreceipt                                          AS STATUSRECEIPT
             , it.markingrefinventtransorigin                            AS MARKINGREFINVENTTRANSORIGIN
             , it.returninventtransorigin                                AS RETURNINVENTTRANSORIGIN
             , it.valueopen                                              AS VALUEOPEN
             , it.inventtransorigin                                      AS INVENTTRANSORIGIN
             , it.voucherphysical                                        AS VOUCHERPHYSICAL
             , it.dataareaid                                            AS DATAAREAID
             , it.recid                                                 AS RECID_IT
             , itp.recid                                                AS RECID_ITPDP
             , 0                                                         AS RECID_ITPDF
             , ist.recid                                                AS RECID_IS
             , ito.inventtransid                                         AS INVENTTRANSID
             , ito.referencecategory                                     AS REFERENCECATEGORY
             , ito.referenceid                                           AS REFERENCE
             , itp.itemid                                                AS ITEMID
             , itp.voucher                                               AS VOUCHER
             , itp.transdate                                             AS TRANSDATE
             , itp.inventtranspostingtype                                AS INVENTTRANSPOSTINGTYPE
             , itp.postingtype                                           AS POSTINGTYPE
             , itp.postingtypeoffset                                     AS POSTINGTYPEOFFSET
             , itp.ledgerdimension                                       AS LEDGERDIMENSION
             , itp.offsetledgerdimension                                 AS LEDGERDIMENSIONOFFSET
             , itp.transbegintime                                        AS TRANSBEGINTIME
             , itp.inventtranspostingtype                                AS UPDATEINVENTTRANSPOSTINGTYPE
             , ist.posted                                                AS ISPOSTED
             , (CAST((0) AS NUMERIC(32, 16)))                            AS QTY
             , (CAST((- (CAST(ist.costamountadjustment AS NUMERIC(32, 16)) )) AS NUMERIC(32, 16))) AS AMOUNT

          FROM {{ ref('inventtrans') }}             it
         INNER JOIN {{ ref('inventtransorigin') }}  ito
            ON ito.dataareaid                  = it.dataareaid
           AND ito.recid                        = it.inventtransorigin
           AND ito.isexcludedfrominventoryvalue = 0
         INNER JOIN {{ ref('inventtransposting') }} itp
            ON itp.dataareaid                  = it.dataareaid
           AND itp.voucher                      = it.voucherphysical
           AND itp.transdate                    = it.datephysical
           AND itp.inventtranspostingtype       = 0
           AND itp.inventtransorigin            = ito.recid
         INNER JOIN {{ ref('inventsettlement') }}   ist
            ON ist.transrecid                   = it.recid
           AND ist.settlemodel                  = 7;
),
inventoryvaluetrans_factinventvaluephysicaltransaction AS (
    SELECT it.qty                         AS QTY
             , it.costamountphysical          AS AMOUNT
             , it.inventdimid                 AS INVENTDIMID
             , it.statusissue                 AS STATUSISSUE
             , it.statusreceipt               AS STATUSRECEIPT
             , it.markingrefinventtransorigin AS MARKINGREFINVENTTRANSORIGIN
             , it.returninventtransorigin     AS RETURNINVENTTRANSORIGIN
             , it.valueopen                   AS VALUEOPEN
             , it.inventtransorigin           AS INVENTTRANSORIGIN
             , it.voucherphysical             AS VOUCHERPHYSICAL
             , it.dataareaid                 AS DATAAREAID
             , it.recid                      AS RECID_IT
             , itp.recid                     AS RECID_ITPDP
             , 0                              AS RECID_ITPDF
             , 0                              AS RECID_IS
             , ito.inventtransid              AS INVENTTRANSID
             , ito.referencecategory          AS REFERENCECATEGORY
             , ito.referenceid                AS REFERENCE
             , itp.itemid                     AS ITEMID
             , itp.voucher                    AS VOUCHER
             , itp.transdate                  AS TRANSDATE
             , itp.inventtranspostingtype     AS INVENTTRANSPOSTINGTYPE
             , itp.isposted                   AS ISPOSTED
             , itp.postingtype                AS POSTINGTYPE
             , itp.postingtypeoffset          AS POSTINGTYPEOFFSET
             , itp.ledgerdimension            AS LEDGERDIMENSION
             , itp.offsetledgerdimension      AS LEDGERDIMENSIONOFFSET
             , itp.transbegintime             AS TRANSBEGINTIME
             , itp.inventtranspostingtype     AS UPDATEINVENTTRANSPOSTINGTYPE

          FROM {{ ref('inventtrans') }}             it
         INNER JOIN {{ ref('inventtransorigin') }}  ito
            ON ito.dataareaid                  = it.dataareaid
           AND ito.recid                        = it.inventtransorigin
           AND ito.isexcludedfrominventoryvalue = 0
         INNER JOIN {{ ref('inventtransposting') }} itp
            ON itp.dataareaid                  = it.dataareaid
           AND itp.voucher                      = it.voucherphysical
           AND itp.transdate                    = it.datephysical
           AND itp.inventtranspostingtype       = 0
           AND itp.inventtransorigin            = ito.recid;
),
inventoryvaluetrans_factinventvaluefinancialtransaction AS (
    SELECT it.qty                         AS QTY
             , it.costamountposted            AS AMOUNT
             , it.inventdimid                 AS INVENTDIMID
             , it.statusissue                 AS STATUSISSUE
             , it.statusreceipt               AS STATUSRECEIPT
             , it.markingrefinventtransorigin AS MARKINGREFINVENTTRANSORIGIN
             , it.returninventtransorigin     AS RETURNINVENTTRANSORIGIN
             , it.valueopen                   AS VALUEOPEN
             , it.inventtransorigin           AS INVENTTRANSORIGIN
             , it.voucherphysical             AS VOUCHERPHYSICAL
             , it.dataareaid                 AS DATAAREAID
             , it.recid                      AS RECID_IT
             , 0                              AS RECID_ITPDP
             , itp.recid                     AS RECID_ITPDF
             , 0                              AS RECID_IS
             , ito.inventtransid              AS INVENTTRANSID
             , ito.referencecategory          AS REFERENCECATEGORY
             , ito.referenceid                AS REFERENCE
             , itp.itemid                     AS ITEMID
             , itp.voucher                    AS VOUCHER
             , itp.transdate                  AS TRANSDATE
             , itp.inventtranspostingtype     AS INVENTTRANSPOSTINGTYPE
             , itp.isposted                   AS ISPOSTED
             , itp.postingtype                AS POSTINGTYPE
             , itp.postingtypeoffset          AS POSTINGTYPEOFFSET
             , itp.ledgerdimension            AS LEDGERDIMENSION
             , itp.offsetledgerdimension      AS LEDGERDIMENSIONOFFSET
             , itp.transbegintime             AS TRANSBEGINTIME
             , itp.inventtranspostingtype     AS UPDATEINVENTTRANSPOSTINGTYPE

          FROM {{ ref('inventtrans') }}             it
         INNER JOIN {{ ref('inventtransorigin') }}  ito
            ON ito.dataareaid                  = it.dataareaid
           AND ito.isexcludedfrominventoryvalue = 0
           AND ito.recid                        = it.inventtransorigin
         INNER JOIN {{ ref('inventtransposting') }} itp
            ON itp.dataareaid                  = it.dataareaid
           AND itp.voucher                      = it.voucher
           AND itp.transdate                    = it.datefinancial
           AND itp.inventtranspostingtype       = 1
           AND itp.inventtransorigin            = ito.recid;
),
inventoryvaluetrans_factinventvaluephysicalreversed AS (
    SELECT it.inventdimid                                         AS INVENTDIMID
             , it.statusissue                                         AS STATUSISSUE
             , it.statusreceipt                                       AS STATUSRECEIPT
             , it.markingrefinventtransorigin                         AS MARKINGREFINVENTTRANSORIGIN
             , it.returninventtransorigin                             AS RETURNINVENTTRANSORIGIN
             , it.valueopen                                           AS VALUEOPEN
             , it.inventtransorigin                                   AS INVENTTRANSORIGIN
             , it.voucherphysical                                     AS VOUCHERPHYSICAL
             , it.dataareaid                                         AS DATAAREAID
             , it.recid                                              AS RECID_IT
             , ist.recid                                             AS RECID_ITPDP
             , itp.recid                                             AS RECID_ITPDF
             , 0                                                      AS RECID_IS
             , ito.inventtransid                                      AS INVENTTRANSID
             , ito.referencecategory                                  AS REFERENCECATEGORY
             , ito.referenceid                                        AS REFERENCE
             , itp.itemid                                             AS ITEMID
             , itp.voucher                                            AS VOUCHER
             , itp.transdate                                          AS TRANSDATE
             , itp.transbegintime                                     AS TRANSBEGINTIME
             , itp.inventtranspostingtype                             AS UPDATEINVENTTRANSPOSTINGTYPE
             , ist.inventtranspostingtype                             AS INVENTTRANSPOSTINGTYPE
             , ist.isposted                                           AS ISPOSTED
             , ist.postingtype                                        AS POSTINGTYPE
             , ist.postingtypeoffset                                  AS POSTINGTYPEOFFSET
             , ist.ledgerdimension                                    AS LEDGERDIMENSION
             , ist.offsetledgerdimension                              AS LEDGERDIMENSIONOFFSET
             , (CAST((- (CAST(it.qty AS NUMERIC(32, 16)))) AS NUMERIC(32, 16)))                AS QTY
             , (CAST((- (CAST(it.costamountphysical AS NUMERIC(32, 16)))) AS NUMERIC(32, 16))) AS AMOUNT

          FROM {{ ref('inventtrans') }}             it
         INNER JOIN {{ ref('inventtransorigin') }}  ito
            ON ito.recid                        = it.inventtransorigin
           AND ito.isexcludedfrominventoryvalue = 0
         INNER JOIN {{ ref('inventtransposting') }} itp
            ON itp.dataareaid                  = it.dataareaid
           AND itp.voucher                      = it.voucher
           AND itp.transdate                    = it.datefinancial
           AND itp.inventtranspostingtype       = 1
           AND itp.inventtransorigin            = ito.recid
         INNER JOIN {{ ref('inventtransposting') }} ist
            ON ist.dataareaid                  = it.dataareaid
           AND ist.voucher                      = it.voucherphysical
           AND ist.transdate                    = it.datephysical
           AND ist.inventtranspostingtype       = 0
           AND ist.inventtransorigin            = ito.recid;
),
inventoryvaluetrans_factinventvaluephysicalandfinancialsettlement AS (
    SELECT it.inventdimid                                                   AS INVENTDIMID
             , it.statusissue                                                   AS STATUSISSUE
             , it.statusreceipt                                                 AS STATUSRECEIPT
             , it.markingrefinventtransorigin                                   AS MARKINGREFINVENTTRANSORIGIN
             , it.returninventtransorigin                                       AS RETURNINVENTTRANSORIGIN
             , it.valueopen                                                     AS VALUEOPEN
             , it.inventtransorigin                                             AS INVENTTRANSORIGIN
             , it.voucherphysical                                               AS VOUCHERPHYSICAL
             , it.dataareaid                                                   AS DATAAREAID
             , it.recid                                                        AS RECID_IT
             , 0                                                                AS RECID_ITPDP
             , 0                                                                AS RECID_ITPDF
             , ist.recid                                                       AS RECID_IS
             , ito.inventtransid                                                AS INVENTTRANSID
             , ito.referencecategory                                            AS REFERENCECATEGORY
             , ito.referenceid                                                  AS REFERENCE
             , ist.costamountadjustment                                         AS AMOUNT
             , ist.itemid                                                       AS ITEMID
             , ist.voucher                                                      AS VOUCHER
             , ist.transdate                                                    AS TRANSDATE
             , ist.posted                                                       AS ISPOSTED
             , ist.balancesheetposting                                          AS POSTINGTYPE
             , ist.operationsposting                                            AS POSTINGTYPEOFFSET
             , ist.balancesheetledgerdimension                                  AS LEDGERDIMENSION
             , ist.operationsledgerdimension                                    AS LEDGERDIMENSIONOFFSET
             , ist.transbegintime                                               AS TRANSBEGINTIME
             , (CAST((0) AS NUMERIC(32, 16)))                                   AS QTY
             , (CAST((CASE WHEN ist.settlemodel = 7 THEN 0 ELSE 1 END) AS INT)) AS INVENTTRANSPOSTINGTYPE
             , (CAST((CASE WHEN ist.settlemodel = 7 THEN 0 ELSE 1 END) AS INT)) AS UPDATEINVENTTRANSPOSTINGTYPE

          FROM {{ ref('inventtrans') }}            it
         INNER JOIN {{ ref('inventtransorigin') }} ito
            ON ito.recid                        = it.inventtransorigin
           AND ito.isexcludedfrominventoryvalue = 0
         INNER JOIN {{ ref('inventsettlement') }}  ist
            ON ist.dataareaid                  = it.dataareaid
           AND ist.transrecid                   = it.recid;
),
inventoryvaluetrans_factinventvaluetransunionall AS (
    SELECT *

          FROM (   SELECT ivpa.AMOUNT                       AS AMOUNT
                        , ivpa.TRANSBEGINTIME               AS TRANSBEGINTIME
                        , ivpa.INVENTDIMID                  AS INVENTDIMID
                        , ivpa.INVENTTRANSID                AS INVENTTRANSID
                        , ivpa.INVENTTRANSPOSTINGTYPE       AS INVENTTRANSPOSTINGTYPE
                        , ivpa.ISPOSTED                     AS ISPOSTED
                        , ivpa.ITEMID                       AS ITEMID
                        , ivpa.LEDGERDIMENSION              AS LEDGERDIMENSION
                        , ivpa.LEDGERDIMENSIONOFFSET        AS LEDGERDIMENSIONOFFSET
                        , ivpa.POSTINGTYPE                  AS POSTINGTYPE
                        , ivpa.POSTINGTYPEOFFSET            AS POSTINGTYPEOFFSET
                        , ivpa.QTY                          AS QTY
                        , ivpa.REFERENCE                    AS REFERENCE
                        , ivpa.REFERENCECATEGORY            AS REFERENCECATEGORY
                        , ivpa.TRANSDATE                    AS TRANSDATE
                        , ivpa.VOUCHER                      AS VOUCHER
                        , ivpa.STATUSISSUE                  AS STATUSISSUE
                        , ivpa.STATUSRECEIPT                AS STATUSRECEIPT
                        , ivpa.MARKINGREFINVENTTRANSORIGIN  AS MARKINGREFINVENTTRANSORIGIN
                        , ivpa.RETURNINVENTTRANSORIGIN      AS RETURNINVENTTRANSORIGIN
                        , ivpa.VALUEOPEN                    AS VALUEOPEN
                        , ivpa.UPDATEINVENTTRANSPOSTINGTYPE AS UPDATEINVENTTRANSPOSTINGTYPE
                        , ivpa.INVENTTRANSORIGIN            AS INVENTTRANSORIGIN
                        , ivpa.VOUCHERPHYSICAL              AS VOUCHERPHYSICAL
                        , ivpa.DATAAREAID                  AS DATAAREAID
                        , ivpa.RECID_IT                     AS RECID_IT
                        , ivpa.RECID_ITPDP                  AS RECID_ITPDP
                        , ivpa.RECID_ITPDF                  AS RECID_ITPDF
                        , ivpa.RECID_IS                     AS RECID_IS
                        , 1                                 AS UNIONALLBRANCHID
                     FROM inventoryvaluetrans_factinventvaluephysicaladjustment ivpa
                   UNION ALL
                   SELECT ivpt.AMOUNT                       AS AMOUNT
                        , ivpt.TRANSBEGINTIME               AS TRANSBEGINTIME
                        , ivpt.INVENTDIMID                  AS INVENTDIMID
                        , ivpt.INVENTTRANSID                AS INVENTTRANSID
                        , ivpt.INVENTTRANSPOSTINGTYPE       AS INVENTTRANSPOSTINGTYPE
                        , ivpt.ISPOSTED                     AS ISPOSTED
                        , ivpt.ITEMID                       AS ITEMID
                        , ivpt.LEDGERDIMENSION              AS LEDGERDIMENSION
                        , ivpt.LEDGERDIMENSIONOFFSET        AS LEDGERDIMENSIONOFFSET
                        , ivpt.POSTINGTYPE                  AS POSTINGTYPE
                        , ivpt.POSTINGTYPEOFFSET            AS POSTINGTYPEOFFSET
                        , ivpt.QTY                          AS QTY
                        , ivpt.REFERENCE                    AS REFERENCE
                        , ivpt.REFERENCECATEGORY            AS REFERENCECATEGORY
                        , ivpt.TRANSDATE                    AS TRANSDATE
                        , ivpt.VOUCHER                      AS VOUCHER
                        , ivpt.STATUSISSUE                  AS STATUSISSUE
                        , ivpt.STATUSRECEIPT                AS STATUSRECEIPT
                        , ivpt.MARKINGREFINVENTTRANSORIGIN  AS MARKINGREFINVENTTRANSORIGIN
                        , ivpt.RETURNINVENTTRANSORIGIN      AS RETURNINVENTTRANSORIGIN
                        , ivpt.VALUEOPEN                    AS VALUEOPEN
                        , ivpt.UPDATEINVENTTRANSPOSTINGTYPE AS UPDATEINVENTTRANSPOSTINGTYPE
                        , ivpt.INVENTTRANSORIGIN            AS INVENTTRANSORIGIN
                        , ivpt.VOUCHERPHYSICAL              AS VOUCHERPHYSICAL
                        , ivpt.DATAAREAID                  AS DATAAREAID
                        , ivpt.RECID_IT                     AS RECID_IT
                        , ivpt.RECID_ITPDP                  AS RECID_ITPDP
                        , ivpt.RECID_ITPDF                  AS RECID_ITPDF
                        , ivpt.RECID_IS                     AS RECID_IS
                        , 2                                 AS UNIONALLBRANCHID
                     FROM inventoryvaluetrans_factinventvaluephysicaltransaction ivpt
                   UNION ALL
                   SELECT ivft.AMOUNT                       AS AMOUNT
                        , ivft.TRANSBEGINTIME               AS TRANSBEGINTIME
                        , ivft.INVENTDIMID                  AS INVENTDIMID
                        , ivft.INVENTTRANSID                AS INVENTTRANSID
                        , ivft.INVENTTRANSPOSTINGTYPE       AS INVENTTRANSPOSTINGTYPE
                        , ivft.ISPOSTED                     AS ISPOSTED
                        , ivft.ITEMID                       AS ITEMID
                        , ivft.LEDGERDIMENSION              AS LEDGERDIMENSION
                        , ivft.LEDGERDIMENSIONOFFSET        AS LEDGERDIMENSIONOFFSET
                        , ivft.POSTINGTYPE                  AS POSTINGTYPE
                        , ivft.POSTINGTYPEOFFSET            AS POSTINGTYPEOFFSET
                        , ivft.QTY                          AS QTY
                        , ivft.REFERENCE                    AS REFERENCE
                        , ivft.REFERENCECATEGORY            AS REFERENCECATEGORY
                        , ivft.TRANSDATE                    AS TRANSDATE
                        , ivft.VOUCHER                      AS VOUCHER
                        , ivft.STATUSISSUE                  AS STATUSISSUE
                        , ivft.STATUSRECEIPT                AS STATUSRECEIPT
                        , ivft.MARKINGREFINVENTTRANSORIGIN  AS MARKINGREFINVENTTRANSORIGIN
                        , ivft.RETURNINVENTTRANSORIGIN      AS RETURNINVENTTRANSORIGIN
                        , ivft.VALUEOPEN                    AS VALUEOPEN
                        , ivft.UPDATEINVENTTRANSPOSTINGTYPE AS UPDATEINVENTTRANSPOSTINGTYPE
                        , ivft.INVENTTRANSORIGIN            AS INVENTTRANSORIGIN
                        , ivft.VOUCHERPHYSICAL              AS VOUCHERPHYSICAL
                        , ivft.DATAAREAID                  AS DATAAREAID
                        , ivft.RECID_IT                     AS RECID_IT
                        , ivft.RECID_ITPDP                  AS RECID_ITPDP
                        , ivft.RECID_ITPDF                  AS RECID_ITPDF
                        , ivft.RECID_IS                     AS RECID_IS
                        , 3                                 AS UNIONALLBRANCHID
                     FROM inventoryvaluetrans_factinventvaluefinancialtransaction ivft
                   UNION ALL
                   SELECT ivpr.AMOUNT                       AS AMOUNT
                        , ivpr.TRANSBEGINTIME               AS TRANSBEGINTIME
                        , ivpr.INVENTDIMID                  AS INVENTDIMID
                        , ivpr.INVENTTRANSID                AS INVENTTRANSID
                        , ivpr.INVENTTRANSPOSTINGTYPE       AS INVENTTRANSPOSTINGTYPE
                        , ivpr.ISPOSTED                     AS ISPOSTED
                        , ivpr.ITEMID                       AS ITEMID
                        , ivpr.LEDGERDIMENSION              AS LEDGERDIMENSION
                        , ivpr.LEDGERDIMENSIONOFFSET        AS LEDGERDIMENSIONOFFSET
                        , ivpr.POSTINGTYPE                  AS POSTINGTYPE
                        , ivpr.POSTINGTYPEOFFSET            AS POSTINGTYPEOFFSET
                        , ivpr.QTY                          AS QTY
                        , ivpr.REFERENCE                    AS REFERENCE
                        , ivpr.REFERENCECATEGORY            AS REFERENCECATEGORY
                        , ivpr.TRANSDATE                    AS TRANSDATE
                        , ivpr.VOUCHER                      AS VOUCHER
                        , ivpr.STATUSISSUE                  AS STATUSISSUE
                        , ivpr.STATUSRECEIPT                AS STATUSRECEIPT
                        , ivpr.MARKINGREFINVENTTRANSORIGIN  AS MARKINGREFINVENTTRANSORIGIN
                        , ivpr.RETURNINVENTTRANSORIGIN      AS RETURNINVENTTRANSORIGIN
                        , ivpr.VALUEOPEN                    AS VALUEOPEN
                        , ivpr.UPDATEINVENTTRANSPOSTINGTYPE AS UPDATEINVENTTRANSPOSTINGTYPE
                        , ivpr.INVENTTRANSORIGIN            AS INVENTTRANSORIGIN
                        , ivpr.VOUCHERPHYSICAL              AS VOUCHERPHYSICAL
                        , ivpr.DATAAREAID                  AS DATAAREAID
                        , ivpr.RECID_IT                     AS RECID_IT
                        , ivpr.RECID_ITPDP                  AS RECID_ITPDP
                        , ivpr.RECID_ITPDF                  AS RECID_ITPDF
                        , ivpr.RECID_IS                     AS RECID_IS
                        , 4                                 AS UNIONALLBRANCHID
                     FROM inventoryvaluetrans_factinventvaluephysicalreversed ivpr
                   UNION ALL
                   SELECT ivpfs.AMOUNT                       AS AMOUNT
                        , ivpfs.TRANSBEGINTIME               AS TRANSBEGINTIME
                        , ivpfs.INVENTDIMID                  AS INVENTDIMID
                        , ivpfs.INVENTTRANSID                AS INVENTTRANSID
                        , ivpfs.INVENTTRANSPOSTINGTYPE       AS INVENTTRANSPOSTINGTYPE
                        , ivpfs.ISPOSTED                     AS ISPOSTED
                        , ivpfs.ITEMID                       AS ITEMID
                        , ivpfs.LEDGERDIMENSION              AS LEDGERDIMENSION
                        , ivpfs.LEDGERDIMENSIONOFFSET        AS LEDGERDIMENSIONOFFSET
                        , ivpfs.POSTINGTYPE                  AS POSTINGTYPE
                        , ivpfs.POSTINGTYPEOFFSET            AS POSTINGTYPEOFFSET
                        , ivpfs.QTY                          AS QTY
                        , ivpfs.REFERENCE                    AS REFERENCE
                        , ivpfs.REFERENCECATEGORY            AS REFERENCECATEGORY
                        , ivpfs.TRANSDATE                    AS TRANSDATE
                        , ivpfs.VOUCHER                      AS VOUCHER
                        , ivpfs.STATUSISSUE                  AS STATUSISSUE
                        , ivpfs.STATUSRECEIPT                AS STATUSRECEIPT
                        , ivpfs.MARKINGREFINVENTTRANSORIGIN  AS MARKINGREFINVENTTRANSORIGIN
                        , ivpfs.RETURNINVENTTRANSORIGIN      AS RETURNINVENTTRANSORIGIN
                        , ivpfs.VALUEOPEN                    AS VALUEOPEN
                        , ivpfs.UPDATEINVENTTRANSPOSTINGTYPE AS UPDATEINVENTTRANSPOSTINGTYPE
                        , ivpfs.INVENTTRANSORIGIN            AS INVENTTRANSORIGIN
                        , ivpfs.VOUCHERPHYSICAL              AS VOUCHERPHYSICAL
                        , ivpfs.DATAAREAID                  AS DATAAREAID
                        , ivpfs.RECID_IT                     AS RECID_IT
                        , ivpfs.RECID_ITPDP                  AS RECID_ITPDP
                        , ivpfs.RECID_ITPDF                  AS RECID_ITPDF
                        , ivpfs.RECID_IS                     AS RECID_IS
                        , 5                                  AS UNIONALLBRANCHID
                     FROM inventoryvaluetrans_factinventvaluephysicalandfinancialsettlement ivpfs) T;
),
inventoryvaluetrans_factinventdim AS (
    SELECT DISTINCT
               id.inventsizeid
             , id.inventcolorid
             , id.inventstyleid
             , id.inventsiteid
             , id.configid
             , id.inventlocationid
             , id.wmslocationid
             , id.inventdimid
             , id.dataareaid
             , id.inventbatchid

          FROM {{ ref('inventtrans') }}    it
         INNER JOIN {{ ref('inventdim') }} id
            ON id.dataareaid = it.dataareaid
           AND id.inventdimid = it.inventdimid;
),
inventoryvaluetrans_factinventtrans AS (
    SELECT it.recid
             , it.statusissue
             , it.statusreceipt
             , ito.referencecategory
             , it.inventdimid
             , it.inventtransorigin
             , it.dataareaid

          FROM {{ ref('inventtrans') }}       it
          JOIN {{ ref('inventtransorigin') }} ito
            ON ito.recid = it.inventtransorigin;
),
inventoryvaluetrans_factstage AS (
    SELECT ivr.INVENTTRANSPOSTINGTYPE
             , ivr.DATAAREAID                                                                                                   AS LegalEntityID
             , id.inventsizeid
             , id.inventcolorid
             , id.inventstyleid
             , id.configid
             , ivr.ITEMID
             , dp.ItemType
             , id.inventlocationid                                                                                              AS WarehouseID
             , id.wmslocationid                                                                                                 AS WarehouseLocationID
             , id.inventsiteid                                                                                                  AS SiteID
             , id.inventbatchid                                                                                                 AS TagID
             , CASE WHEN it.statusissue > 0 THEN 1 ELSE 2 END                                                                   AS TransStatusTypeID
             , CASE WHEN it.statusissue > 0 THEN it.statusissue ELSE it.statusreceipt END                                       AS TransStatusID
             , ivr.TRANSDATE                                                                                                    AS TransDate
             , ivr.LEDGERDIMENSION                                                                                              AS LedgerDimensionID
             , ivr.LEDGERDIMENSIONOFFSET                                                                                        AS LedgerDimensionOffsetID
             , ivr.VOUCHER                                                                                                      AS VoucherID
             , it.inventtransorigin                                                                                             AS RECID_ITO
             , CASE WHEN dp.ItemType = 'Service' THEN 0 ELSE ivr.AMOUNT END                                                     AS InventoryAmount
             , CASE WHEN dp.ItemType = 'Service' THEN 0 ELSE ivr.QTY END                                                        AS InventoryQuantity
             , CASE WHEN ivr.INVENTTRANSPOSTINGTYPE = 0 
                     AND ivr.ISPOSTED = 1
                     AND dp.ItemType <> 'Service'
                    THEN ivr.QTY
                    ELSE 0 END                                                                                                  AS InventoryPhysicalPostedQuantity
             , CASE WHEN ivr.INVENTTRANSPOSTINGTYPE = 0 
                     AND ivr.ISPOSTED = 1
                     AND dp.ItemType <> 'Service'
                    THEN ivr.AMOUNT
                    ELSE 0 END                                                                                                  AS InventoryPhysicalPostedAmount
             , CASE WHEN ivr.INVENTTRANSPOSTINGTYPE = 0 AND ivr.ISPOSTED = 0 AND dp.ItemType <> 'Service' THEN ivr.QTY ELSE
                                                                                                                       0 END    AS InventoryPhysicalNonPostedQuantity
             , CASE WHEN ivr.INVENTTRANSPOSTINGTYPE = 0 AND ivr.ISPOSTED = 0 AND dp.ItemType <> 'Service' THEN ivr.AMOUNT ELSE
                                                                                                                          0 END AS InventoryPhysicalNonPostedAmount
             , CASE WHEN ivr.INVENTTRANSPOSTINGTYPE = 0
                     AND it.referencecategory IN ( 2, 8, 27, 28, 29, 30, 31, 32, 100 )
                    THEN ivr.AMOUNT
                    ELSE 0 END                                                                                                  AS WIPAmount
             , CASE WHEN ivr.INVENTTRANSPOSTINGTYPE = 0
                     AND it.referencecategory IN ( 2, 8, 27, 28, 29, 30, 31, 32, 100 )
                    THEN ivr.QTY
                    ELSE 0 END                                                                                                  AS WIPQuantity
             , ivr.RECID_IT                                                                                                     AS RECID_IT
             , ivr.RECID_ITPDP                                                                                                  AS RECID_ITPDP
             , ivr.RECID_ITPDF                                                                                                  AS RECID_ITPDF
             , ivr.RECID_IS                                                                                                     AS RECID_IS

          FROM inventoryvaluetrans_factinventvaluetransunionall ivr
          LEFT JOIN inventoryvaluetrans_factinventtrans         it
            ON it.recid         = ivr.RECID_IT
          LEFT JOIN inventoryvaluetrans_factinventdim           id
            ON id.dataareaid   = ivr.DATAAREAID
           AND id.inventdimid   = ivr.INVENTDIMID
          LEFT JOIN silver.cma_Product          dp
            ON dp.LegalEntityID = ivr.DATAAREAID
           AND dp.ItemID        = ivr.ITEMID
           AND dp.ProductWidth  = id.inventsizeid
           AND dp.ProductLength = id.inventcolorid
           AND dp.ProductColor  = id.inventstyleid
           AND dp.ProductConfig = id.configid
         WHERE ivr.TRANSDATE BETWEEN '1990-01-01' AND '9999-12-31';
),
inventoryvaluetrans_factfinancial AS (
    SELECT (CASE WHEN ts.INVENTTRANSPOSTINGTYPE = 1 
                      AND ts.ItemType <> 'Service' 
                     THEN ts.InventoryQuantity
                     ELSE 0 END) AS InventoryFinancialQuantity
             , CASE WHEN ts.INVENTTRANSPOSTINGTYPE = 1 
                     AND ts.ItemType <> 'Service' 
                    THEN ts.InventoryAmount
                    ELSE 0 END   AS InventoryFinancialAmount
             , ts.RECID_IT       AS RECID_IT
             , ts.RECID_ITPDP    AS RECID_ITPDP
             , ts.RECID_ITPDF    AS RECID_ITPDF
             , ts.RECID_IS       AS RECID_IS

          FROM inventoryvaluetrans_factstage ts;
),
inventoryvaluetrans_factdetail1 AS (
    SELECT ISNULL(dp.ProductKey, -1)               AS ProductKey
             , ISNULL(it.inventorytranskey, -1)        AS InventoryTransKey
             , ISNULL(dca.LedgerAccountKey, -1)        AS LedgerAccountKey
             , ISNULL(dca1.LedgerAccountKey, -1)       AS LedgerAccountOffsetKey
             , ISNULL(d.DateKey, -1)                   AS TransDateKey
             , ISNULL(dw.WarehouseKey, -1)             AS WarehouseKey
             , ISNULL(dwl.WarehouseLocationKey, -1)    AS WarehouseLocationKey
             , ISNULL(dt.TagKey, -1)                   AS TagKey
             , ISNULL(le.LegalEntityKey, -1)           AS LegalEntityKey
             , ISNULL(dts.InventoryTransStatusKey, -1) AS InventoryTransStatusKey
             , ISNULL(vou.VoucherKey, -1)              AS VoucherKey
             , ISNULL(ds.InventorySiteKey, -1)         AS InventorySiteKey
             , ISNULL(il.LotKey, -1)                   AS LotKey
             , df.InventoryFinancialQuantity           AS InventoryFinancialQuantity
             , df.InventoryFinancialAmount             AS InventoryFinancialAmount
             , ts.InventoryPhysicalPostedAmount        AS InventoryPhysicalPostedAmount
             , ts.InventoryPhysicalPostedQuantity      AS InventoryPhysicalPostedQuantity
             , ts.InventoryPhysicalNonPostedQuantity   AS InventoryPhysicalNonPostedQuantity
             , ts.InventoryPhysicalNonPostedAmount     AS InventoryPhysicalNonPostedAmount
             , dp.InventoryUOM
             , ts.InventoryAmount
             , ts.InventoryQuantity
             , ts.WIPAmount * -1                       AS WIPAmount
             , ts.WIPQuantity * -1                     AS WIPQuantity
             , ts.RECID_IT                             AS RECID_IT
             , ts.RECID_ITPDP                          AS RECID_ITPDP
             , ts.RECID_IS                             AS RECID_IS
             , ts.RECID_ITPDF                          AS RECID_ITPDF
             , 1                                       AS _SourceID

          FROM inventoryvaluetrans_factstage                        ts
         INNER JOIN silver.cma_LegalEntity          le
            ON le.LegalEntityID               = ts.LegalEntityID
          LEFT JOIN inventoryvaluetrans_factfinancial               df
            ON df.RECID_IT                    = ts.RECID_IT
           AND df.RECID_ITPDP                 = ts.RECID_ITPDP
           AND df.RECID_ITPDF                 = ts.RECID_ITPDF
           AND df.RECID_IS                    = ts.RECID_IS
          LEFT JOIN silver.cma_InventoryTrans_Fact  it
            ON it._recid                      = ts.RECID_IT
           AND it._sourceid                   = 1
          LEFT JOIN silver.cma_Product              dp
            ON dp.LegalEntityID               = ts.LegalEntityID
           AND dp.ItemID                      = ts.ITEMID
           AND dp.ProductWidth                = ts.INVENTSIZEID
           AND dp.ProductLength               = ts.INVENTCOLORID
           AND dp.ProductColor                = ts.INVENTSTYLEID
           AND dp.ProductConfig               = ts.CONFIGID
          LEFT JOIN silver.cma_Warehouse            dw
            ON dw.LegalEntityID               = ts.LegalEntityID
           AND dw.WarehouseID                 = ts.WarehouseID
          LEFT JOIN silver.cma_WarehouseLocation    dwl
            ON dwl.LegalEntityID              = ts.LegalEntityID
           AND dwl.WarehouseID                = ts.WarehouseID
           AND dwl.WarehouseLocation          = ts.WarehouseLocationID
          LEFT JOIN silver.cma_Tag                  dt
            ON dt.LegalEntityID               = ts.LegalEntityID
           AND dt.TagID                       = ts.TagID
           AND dt.ItemID                      = ts.ITEMID
          LEFT JOIN silver.cma_LedgerAccount        dca
            ON dca._RecID                     = ts.LedgerDimensionID
           AND dca._SourceID                  = 1
          LEFT JOIN silver.cma_LedgerAccount        dca1
            ON dca1._RecID                    = ts.LedgerDimensionOffsetID
           AND dca1._SourceID                 = 1
          LEFT JOIN silver.cma_InventoryTransStatus dts
            ON dts.InventoryTransStatusTypeID = ts.TransStatusTypeID
           AND dts.InventoryTransStatusID     = ts.TransStatusID
          LEFT JOIN silver.cma_Voucher              vou
            ON vou.LegalEntityID              = ts.LegalEntityID
           AND vou.VoucherID                  = ts.VoucherID
          LEFT JOIN silver.cma_Date                 d
            ON d.Date                         = ts.TransDate
          LEFT JOIN silver.cma_Lot                  il
            ON il._RecID                      = ts.RECID_ITO
           AND il._SourceID                   = 1
          LEFT JOIN silver.cma_InventorySite        ds
            ON ds.LegalEntityID               = ts.LegalEntityID
           AND ds.InventorySiteID             = ts.SiteID
         WHERE df.InventoryFinancialQuantity         <> 0
            OR df.InventoryFinancialAmount           <> 0
            OR ts.InventoryPhysicalPostedQuantity    <> 0
            OR ts.InventoryPhysicalPostedAmount      <> 0
            OR ts.InventoryPhysicalNonPostedAmount   <> 0
            OR ts.InventoryPhysicalNonPostedQuantity <> 0
            OR ts.InventoryAmount                    <> 0
            OR ts.InventoryQuantity                  <> 0
            OR ts.WIPAmount                          <> 0
            OR ts.WIPQuantity                        <> 0;
),
inventoryvaluetrans_factuomconversion AS (
    SELECT td.RECID_IT																  AS _RecID
             , td.RECID_ITPDP															  AS _RecID2
             , td.RECID_IS																  AS _RecID3
             , td.RECID_ITPDF															  AS _RecID4
             , (td.InventoryFinancialQuantity * ISNULL(vuc1.factor, 0) )                   AS InventoryFinancialQuantity_FT

             , (td.InventoryFinancialQuantity * ISNULL(vuc3.factor, 0))		                  AS InventoryFinancialQuantity_LB
             , ROUND((td.InventoryFinancialQuantity * ISNULL(vuc4.factor, 0)), 0)         AS InventoryFinancialQuantity_PC
             , (td.InventoryFinancialQuantity * ISNULL(vuc5.factor, 0))                   AS InventoryFinancialQuantity_SQIN

             , td.InventoryPhysicalPostedQuantity										  AS InventoryPhysicalPostedQuantity
             , (td.InventoryPhysicalPostedQuantity * ISNULL(vuc1.factor, 0) )              AS InventoryPhysicalPostedQuantity_FT

             , (td.InventoryPhysicalPostedQuantity * ISNULL(vuc3.factor, 0))              AS InventoryPhysicalPostedQuantity_LB
             , ROUND((td.InventoryPhysicalPostedQuantity * ISNULL(vuc4.factor, 0)), 0)    AS InventoryPhysicalPostedQuantity_PC
             , (td.InventoryPhysicalPostedQuantity * ISNULL(vuc5.factor, 0))              AS InventoryPhysicalPostedQuantity_SQIN

             , (td.InventoryPhysicalNonPostedQuantity * ISNULL(vuc1.factor, 0) )           AS InventoryPhysicalNonPostedQuantity_FT

             , (td.InventoryPhysicalNonPostedQuantity * ISNULL(vuc3.factor, 0))           AS InventoryPhysicalNonPostedQuantity_LB
             , ROUND((td.InventoryPhysicalNonPostedQuantity * ISNULL(vuc4.factor, 0)), 0) AS InventoryPhysicalNonPostedQuantity_PC
             , (td.InventoryPhysicalNonPostedQuantity * ISNULL(vuc5.factor, 0))           AS InventoryPhysicalNonPostedQuantity_SQIN

             , (td.InventoryQuantity * ISNULL(vuc1.factor, 0) )                            AS InventoryQuantity_FT

             , (td.InventoryQuantity * ISNULL(vuc3.factor, 0))                            AS InventoryQuantity_LB
             , ROUND((td.InventoryQuantity * ISNULL(vuc4.factor, 0)), 0)                  AS InventoryQuantity_PC
             , (td.InventoryQuantity * ISNULL(vuc5.factor, 0))                            AS InventoryQuantity_SQIN

             , td.WIPQuantity * ISNULL(vuc1.factor, 0)                                     AS WIPQuantity_FT

             , td.WIPQuantity * ISNULL(vuc3.factor, 0)                                    AS WIPQuantity_LB
             , td.WIPQuantity * ISNULL(vuc4.factor, 0)                                    AS WIPQuantity_PC
             , td.WIPQuantity * ISNULL(vuc5.factor, 0)                                    AS WIPQuantity_SQIN


          FROM inventoryvaluetrans_factdetail1                 td
          LEFT JOIN {{ ref('vwuomconversion_ft') }} vuc1
            ON vuc1.legalentitykey = td.LegalEntityKey
           AND vuc1.productkey     = td.ProductKey
           AND vuc1.fromuom        = td.InventoryUOM
        -- AND vuc1.touom          = 'FT'





          LEFT JOIN {{ ref('vwuomconversion_lb') }} vuc3
            ON vuc3.legalentitykey = td.LegalEntityKey
           AND vuc3.productkey     = td.ProductKey
           AND vuc3.fromuom        = td.InventoryUOM
        -- AND vuc3.touom          = 'LB'
          LEFT JOIN {{ ref('vwuomconversion_pc') }} vuc4
            ON vuc4.legalentitykey = td.LegalEntityKey
           AND vuc4.productkey     = td.ProductKey
           AND vuc4.fromuom        = td.InventoryUOM
        -- AND vuc4.touom          = 'PC'
          LEFT JOIN {{ ref('vwuomconversion_sqin') }} vuc5
            ON vuc5.legalentitykey = td.LegalEntityKey
           AND vuc5.productkey     = td.ProductKey
           AND vuc5.fromuom        = td.InventoryUOM
        -- AND vuc5.touom          = 'SQIN'
)
SELECT  ROW_NUMBER() OVER (ORDER BY ts.RECID_IT, ts.RECID_ITPDP, ts.RECID_IS, ts.RECID_ITPDF) AS InventoryValueTransKey
         , ts.InventoryTransStatusKey                 AS InventoryTransStatusKey
         , ts.LegalEntityKey                          AS LegalEntityKey
         , ts.LotKey                                  AS LotKey
         , ts.LedgerAccountKey                        AS LedgerAccountKey
         , ts.LedgerAccountOffsetKey                  AS LedgerAccountOffsetKey
         , ts.InventoryTransKey                       AS InventoryTransKey
         , ts.ProductKey                              AS ProductKey
         , ts.InventorySiteKey                        AS InventorySiteKey
         , ts.TagKey                                  AS TagKey
         , ts.TransDateKey                            AS TransDateKey
         , ts.VoucherKey                              AS VoucherKey
         , ts.WarehouseKey                            AS WarehouseKey
         , ts.WarehouseLocationKey                    AS WarehouseLocationKey
         , ts.InventoryFinancialQuantity              AS InventoryFinancialQuantity
         , uc.InventoryFinancialQuantity_FT           AS InventoryFinancialQuantity_FT

         , uc.InventoryFinancialQuantity_LB           AS InventoryFinancialQuantity_LB
         , uc.InventoryFinancialQuantity_PC           AS InventoryFinancialQuantity_PC
         , uc.InventoryFinancialQuantity_SQIN         AS InventoryFinancialQuantity_SQIN

         , ts.InventoryFinancialAmount                AS InventoryFinancialAmount
         , ts.InventoryPhysicalPostedQuantity         AS InventoryPhysicalPostedQuantity
         , uc.InventoryPhysicalPostedQuantity_FT      AS InventoryPhysicalPostedQuantity_FT

         , uc.InventoryPhysicalPostedQuantity_LB      AS InventoryPhysicalPostedQuantity_LB
         , uc.InventoryPhysicalPostedQuantity_PC      AS InventoryPhysicalPostedQuantity_PC
         , uc.InventoryPhysicalPostedQuantity_SQIN    AS InventoryPhysicalPostedQuantity_SQIN

         , ts.InventoryPhysicalPostedAmount           AS InventoryPhysicalPostedAmount
         , ts.InventoryPhysicalNonPostedQuantity      AS InventoryPhysicalNonPostedQuantity
         , uc.InventoryPhysicalNonPostedQuantity_FT   AS InventoryPhysicalNonPostedQuantity_FT

         , uc.InventoryPhysicalNonPostedQuantity_LB   AS InventoryPhysicalNonPostedQuantity_LB
         , uc.InventoryPhysicalNonPostedQuantity_PC   AS InventoryPhysicalNonPostedQuantity_PC
         , uc.InventoryPhysicalNonPostedQuantity_SQIN AS InventoryPhysicalNonPostedQuantity_SQIN

         , ts.InventoryPhysicalNonPostedAmount        AS InventoryPhysicalNonPostedAmount
         , ts.InventoryAmount                         AS InventoryAmount
         , ts.InventoryQuantity                       AS InventoryQuantity
         , uc.InventoryQuantity_FT                    AS InventoryQuantity_FT

         , uc.InventoryQuantity_LB                    AS InventoryQuantity_LB
         , uc.InventoryQuantity_PC                    AS InventoryQuantity_PC
         , uc.InventoryQuantity_SQIN                  AS InventoryQuantity_SQIN

         , ts.WIPAmount                               AS WIPAmount
         , ts.WIPQuantity                             AS WIPQuantity
         , uc.WIPQuantity_FT                          AS WIPQuantity_FT

         , uc.WIPQuantity_LB                          AS WIPQuantity_LB
         , uc.WIPQuantity_PC                          AS WIPQuantity_PC
         , uc.WIPQuantity_SQIN                        AS WIPQuantity_SQIN

         , ts.RECID_IT                                AS _RecID
         , ts.RECID_ITPDP                             AS _RecID2
         , ts.RECID_IS                                AS _RecID3
         , ts.RECID_ITPDF                             AS _RecID4
         , 1                                          AS _SourceID
         ,  CURRENT_TIMESTAMP  AS  _CreatedDate
         , CURRENT_TIMESTAMP AS _ModifiedDate

      FROM inventoryvaluetrans_factdetail1            ts
      LEFT JOIN inventoryvaluetrans_factuomconversion uc
        ON uc._RecID = ts.RECID_IT
       AND uc._RecID2 = ts.RECID_ITPDP
       AND uc._RecID3 = ts.RECID_IS
       AND uc._RecID4 = ts.RECID_ITPDF;
