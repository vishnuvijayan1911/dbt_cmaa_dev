{{ config(materialized='table', tags=['silver'], alias='inventoryvaluebalance_fact') }}

-- Source file: cma/cma/layers/_base/_silver/inventoryvaluebalance_f/inventoryvaluebalance_f.py
-- Root method: InventoryvaluebalanceFact.get_detail [InventoryValueBalance_FactDetail]
-- Inlined methods: InventoryvaluebalanceFact.get_invent_value_physical_transaction [InventoryValueBalance_FactInventValuePhysicalTransaction], InventoryvaluebalanceFact.get_invent_aging_recipt_view [InventoryValueBalance_FactInventAgingReceiptView], InventoryvaluebalanceFact.get_invent_dim [InventoryValueBalance_FactInventDim], InventoryvaluebalanceFact.get_invent_table [InventoryValueBalance_FactInventTable], InventoryvaluebalanceFact.get_fact_date [InventoryValueBalance_FactDate], InventoryvaluebalanceFact.get_invent_value_physical_balance [InventoryValueBalance_FactInventValuePhysicalBalance], InventoryvaluebalanceFact.get_invent_value_financial_balance [InventoryValueBalance_FactInventValueFinancialBalance], InventoryvaluebalanceFact.get_invent_value_physical_adjustment [InventoryValueBalance_FactInventValuePhysicalAdjustment], InventoryvaluebalanceFact.get_invent_value_financial_transaction [InventoryValueBalance_FactInventValueFinancialTransaction], InventoryvaluebalanceFact.get_invent_value_physical_reserved [InventoryValueBalance_FactInventValuePhysicalReversed], InventoryvaluebalanceFact.get_invent_value_physical_financial_settlement [InventoryValueBalance_FactInventValuePhysicalAndFinancialSettlement], InventoryvaluebalanceFact.get_invent_trans_union_all [InventoryValueBalance_FactInventValueTransUnionAll], InventoryvaluebalanceFact.get_invent_value_report_view [InventoryValueBalance_FactInventValueReportView], InventoryvaluebalanceFact.get_invent_value_interim_table [InventoryValueBalance_FactInventValueInterimTable], InventoryvaluebalanceFact.get_invent_value [InventoryValueBalance_FactInventValue], InventoryvaluebalanceFact.get_fact_stage1 [InventoryValueBalance_FactStage1], InventoryvaluebalanceFact.get_fact_stage [InventoryValueBalance_FactStage], InventoryvaluebalanceFact.get_fact_date_445 [InventoryValueBalance_FactDate445], InventoryvaluebalanceFact.get_activity_months [InventoryValueBalance_FactActivityMonths], InventoryvaluebalanceFact.get_fact_totals [InventoryValueBalance_FactTotals], InventoryvaluebalanceFact.get_opening_balance [InventoryValueBalance_FactOpeningBalance], InventoryvaluebalanceFact.get_detail1 [InventoryValueBalance_FactDetail1], InventoryvaluebalanceFact.get_uom_conversion [InventoryValueBalance_FactUOMCovnversion]
-- external_table_name: InventoryValueBalance_FactDetail
-- schema_name: temp

WITH
inventoryvaluebalance_factinventvaluephysicaltransaction AS (
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
        , it.recid                      AS INVENTTRANSRECID
        , it.dataareaid                 AS DATAAREAID
        , it.partition                   AS PARTITION
        , it.recid                      AS RECID
        , ito.inventtransid              AS INVENTTRANSID
        , ito.referencecategory          AS REFERENCECATEGORY
        , ito.referenceid                AS REFERENCE
        , ito.dataareaid                AS DATAAREAID#2
        , ito.partition                  AS PARTITION#2
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
        , itp.dataareaid                AS DATAAREAID#3
        , itp.partition                  AS PARTITION#3
      FROM {{ ref('inventtrans') }}             it
    INNER JOIN {{ ref('inventtransorigin') }}  ito
        ON ito.dataareaid            = it.dataareaid
      AND ito.recid                 = it.inventtransorigin
    --  AND ito.isexcludedfrominventoryvalue = 0
    INNER JOIN {{ ref('inventtransposting') }} itp
        ON itp.dataareaid            = it.dataareaid
      AND itp.voucher                = it.voucherphysical
      AND itp.transdate              = it.datephysical
      AND itp.inventtranspostingtype = 0
      AND itp.inventtransorigin      = ito.recid;
),
inventoryvaluebalance_factinventagingreceiptview AS (
    SELECT T1.ITEMID      AS ITEMID
        , T1.TRANSDATE   AS TRANSDATE
        , T1.QTY         AS QTY
        , T1.INVENTDIMID AS INVENTDIMID
        , T1.DATAAREAID  AS DATAAREAID
        , T1.PARTITION   AS PARTITION
        , T1.RECID       AS RECID
        , id.configid
        , id.inventsizeid
        , id.inventcolorid
        , id.inventstyleid
        , id.inventsiteid
        , id.inventlocationid
        , id.inventbatchid
        , id.licenseplateid
        , id.wmslocationid
      FROM inventoryvaluebalance_factinventvaluephysicaltransaction T1
    INNER JOIN {{ ref('inventdim') }}              id
        ON id.dataareaid = T1.DATAAREAID
      AND id.inventdimid = T1.INVENTDIMID;
),
inventoryvaluebalance_factinventdim AS (
    SELECT id.inventsizeid
        , id.inventcolorid
        , id.inventstyleid
        , id.inventsiteid
        , id.configid
        , id.inventlocationid
        , id.wmslocationid
        , id.inventdimid
        , id.dataareaid
        , id.inventbatchid
        , id.licenseplateid
      FROM {{ ref('inventdim') }} id;
),
inventoryvaluebalance_factinventtable AS (
    SELECT DATAAREAID, ITEMID, ITEMTYPE
    FROM {{ ref('inventtable') }};
),
inventoryvaluebalance_factdate AS (
    SELECT FiscalMonthDate
        , CAST(MIN(FiscalDate) AS DATE) AS StartDate
        , CAST(MAX(FiscalDate) AS DATE) AS EndDate
      FROM {{ ref('date_d') }}
    WHERE FiscalYearDate > DATEADD(YEAR, -3, GETDATE())
    GROUP BY FiscalMonthDate
    ORDER BY FiscalMonthDate;
),
inventoryvaluebalance_factinventvaluephysicalbalance AS (
    SELECT it.itemid                                              AS ITEMID
        , it.inventdimid                                         AS INVENTDIMID
        , it.dataareaid                                         AS DATAAREAID
        , it.partition                                           AS PARTITION
        , it.recid                                              AS RECID
        , ito.referencecategory                                  AS REFERENCECATEGORY
        , ito.dataareaid                                        AS DATAAREAID#2
        , ito.partition                                          AS PARTITION#2
        , itp.inventtranspostingtype                             AS INVENTTRANSPOSTINGTYPE
        , itp.isposted                                           AS ISPOSTED
        , itp.dataareaid                                        AS DATAAREAID#3
        , itp.partition                                          AS PARTITION#3
        , (CAST((- (it.qty)) AS NUMERIC(32, 6)))                 AS QTY
        , (CAST((- (it.costamountphysical)) AS NUMERIC(32, 6)))  AS AMOUNT
        , (CAST(('') AS VARCHAR(20)))                           AS VOUCHER
        , (CAST(({ TS '2154-12-31 00:00:00.000' }) AS DATETIME)) AS TRANSDATE
        , (CAST(('') AS VARCHAR(20)))                           AS INVENTTRANSID
        , (CAST((0) AS INT))                                     AS POSTINGTYPE
        , (CAST((0) AS INT))                                     AS POSTINGTYPEOFFSET
        , (CAST(('') AS VARCHAR(20)))                           AS REFERENCE
        , (CAST((0) AS BIGINT))                                  AS LEDGERDIMENSION
        , (CAST((0) AS BIGINT))                                  AS LEDGERDIMENSIONOFFSET
        , (CAST(('2154-12-31 23:59:59') AS DATETIME))            AS TRANSBEGINTIME
      FROM {{ ref('inventtrans') }}             it
    INNER JOIN {{ ref('inventtransorigin') }}  ito
        ON it.dataareaid       = ito.dataareaid
      AND it.inventtransorigin = ito.recid
    INNER JOIN {{ ref('inventtransposting') }} itp
        ON it.dataareaid       = itp.dataareaid
      AND ito.recid           = itp.inventtransorigin
      AND it.voucherphysical   = itp.voucher
      AND it.datephysical      = itp.transdate
    WHERE ((it.statusissue           = 2 AND it.statusreceipt = 0) OR (it.statusreceipt = 2 AND it.statusissue = 0))
      -- AND ito.isexcludedfrominventoryvalue = 0
      AND itp.inventtranspostingtype = 0;
),
inventoryvaluebalance_factinventvaluefinancialbalance AS (
    SELECT iv.itemid                                              AS ITEMID
        , iv.inventdimid                                         AS INVENTDIMID
        , iv.dataareaid                                         AS DATAAREAID
        , iv.partition                                           AS PARTITION
        , iv.recid                                              AS RECID
        , (CAST((- (iv.postedqty)) AS NUMERIC(32, 6)))           AS QTY
        , (CAST((- (iv.postedvalue)) AS NUMERIC(32, 6)))         AS AMOUNT
        , (CAST(('') AS VARCHAR(20)))                           AS VOUCHER
        , (CAST(({ TS '2154-12-31 00:00:00.000' }) AS DATETIME)) AS TRANSDATE
        , (CAST(('') AS VARCHAR(20)))                           AS INVENTTRANSID
        , (CAST((7) AS INT))                                     AS REFERENCECATEGORY
        , (CAST((1) AS INT))                                     AS INVENTTRANSPOSTINGTYPE
        , (CAST((1) AS INT))                                     AS ISPOSTED
        , (CAST((0) AS INT))                                     AS POSTINGTYPE
        , (CAST((0) AS INT))                                     AS POSTINGTYPEOFFSET
        , (CAST(('') AS VARCHAR(20)))                           AS REFERENCE
        , (CAST((0) AS BIGINT))                                  AS LEDGERDIMENSION
        , (CAST((0) AS BIGINT))                                  AS LEDGERDIMENSIONOFFSET
        , (CAST(('2154-12-31 23:59:59') AS DATETIME))            AS TRANSBEGINTIME
      FROM {{ ref('inventsum') }} iv
    WHERE iv.closed = 0;
),
inventoryvaluebalance_factinventvaluephysicaladjustment AS (
    SELECT it.inventdimid                                           AS INVENTDIMID
        , it.statusissue                                           AS STATUSISSUE
        , it.statusreceipt                                         AS STATUSRECEIPT
        , it.markingrefinventtransorigin                           AS MARKINGREFINVENTTRANSORIGIN
        , it.returninventtransorigin                               AS RETURNINVENTTRANSORIGIN
        , it.valueopen                                             AS VALUEOPEN
        , it.inventtransorigin                                     AS INVENTTRANSORIGIN
        , it.voucherphysical                                       AS VOUCHERPHYSICAL
        , it.recid                                                AS INVENTTRANSRECID
        , it.dataareaid                                           AS DATAAREAID
        , it.partition                                             AS PARTITION
        , it.recid                                                AS RECID
        , ito.inventtransid                                        AS INVENTTRANSID
        , ito.referencecategory                                    AS REFERENCECATEGORY
        , ito.referenceid                                          AS REFERENCE
        , ito.dataareaid                                          AS DATAAREAID#2
        , ito.partition                                            AS PARTITION#2
        , itp.itemid                                               AS ITEMID
        , itp.voucher                                              AS VOUCHER
        , itp.transdate                                            AS TRANSDATE
        , itp.inventtranspostingtype                               AS INVENTTRANSPOSTINGTYPE
        , itp.postingtype                                          AS POSTINGTYPE
        , itp.postingtypeoffset                                    AS POSTINGTYPEOFFSET
        , itp.ledgerdimension                                      AS LEDGERDIMENSION
        , itp.offsetledgerdimension                                AS LEDGERDIMENSIONOFFSET
        , itp.transbegintime                                       AS TRANSBEGINTIME
        , itp.inventtranspostingtype                               AS UPDATEINVENTTRANSPOSTINGTYPE
        , itp.dataareaid                                          AS DATAAREAID#3
        , itp.partition                                            AS PARTITION#3
        , ist.posted                                               AS ISPOSTED
        , ist.dataareaid                                          AS DATAAREAID#4
        , ist.partition                                            AS PARTITION#4
        , (CAST((0) AS NUMERIC(32, 6)))                            AS QTY
        , (CAST((- (CAST(ist.costamountadjustment AS  NUMERIC(32, 6)))) AS NUMERIC(32, 6))) AS AMOUNT
      FROM {{ ref('inventtrans') }}             it
    INNER JOIN {{ ref('inventtransorigin') }}  ito
        ON ito.recid                 = it.inventtransorigin
      AND ito.dataareaid            = it.dataareaid
    -- AND ito.isexcludedfrominventoryvalue = 0
    INNER JOIN {{ ref('inventtransposting') }} itp
        ON itp.dataareaid            = it.dataareaid
      AND itp.voucher                = it.voucherphysical
      AND itp.transdate              = it.datephysical
      AND itp.inventtranspostingtype = 0
      AND itp.inventtransorigin      = ito.recid
    INNER JOIN {{ ref('inventsettlement') }}   ist
        ON ist.transrecid             = it.recid
      AND ist.settlemodel            = 7;
),
inventoryvaluebalance_factinventvaluefinancialtransaction AS (
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
        , it.recid                      AS INVENTTRANSRECID
        , it.dataareaid                 AS DATAAREAID
        , it.partition                   AS PARTITION
        , it.recid                      AS RECID
        , ito.inventtransid              AS INVENTTRANSID
        , ito.referencecategory          AS REFERENCECATEGORY
        , ito.referenceid                AS REFERENCE
        , ito.dataareaid                AS DATAAREAID#2
        , ito.partition                  AS PARTITION#2
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
        , itp.dataareaid                AS DATAAREAID#3
        , itp.partition                  AS PARTITION#3
      FROM {{ ref('inventtrans') }}             it
    INNER JOIN {{ ref('inventtransorigin') }}  ito
        ON ito.dataareaid            = it.dataareaid
      --  AND ito.isexcludedfrominventoryvalue = 0
      AND ito.recid                 = it.inventtransorigin
    INNER JOIN {{ ref('inventtransposting') }} itp
        ON itp.dataareaid            = it.dataareaid
      AND itp.voucher                = it.voucher
      AND itp.transdate              = it.datefinancial
      AND itp.inventtranspostingtype = 1
      AND itp.inventtransorigin      = ito.recid;
),
inventoryvaluebalance_factinventvaluephysicalreversed AS (
    SELECT it.inventdimid                                        AS INVENTDIMID
        , it.statusissue                                        AS STATUSISSUE
        , it.statusreceipt                                      AS STATUSRECEIPT
        , it.markingrefinventtransorigin                        AS MARKINGREFINVENTTRANSORIGIN
        , it.returninventtransorigin                            AS RETURNINVENTTRANSORIGIN
        , it.valueopen                                          AS VALUEOPEN
        , it.inventtransorigin                                  AS INVENTTRANSORIGIN
        , it.voucherphysical                                    AS VOUCHERPHYSICAL
        , it.recid                                             AS INVENTTRANSRECID
        , it.dataareaid                                        AS DATAAREAID
        , it.partition                                          AS PARTITION
        , it.recid                                             AS RECID
        , ito.inventtransid                                     AS INVENTTRANSID
        , ito.referencecategory                                 AS REFERENCECATEGORY
        , ito.referenceid                                       AS REFERENCE
        , ito.dataareaid                                       AS DATAAREAID#2
        , ito.partition                                         AS PARTITION#2
        , itp.itemid                                            AS ITEMID
        , itp.voucher                                           AS VOUCHER
        , itp.transdate                                         AS TRANSDATE
        , itp.transbegintime                                    AS TRANSBEGINTIME
        , itp.inventtranspostingtype                            AS UPDATEINVENTTRANSPOSTINGTYPE
        , itp.dataareaid                                       AS DATAAREAID#3
        , itp.partition                                         AS PARTITION#3
        , itp2.inventtranspostingtype                           AS INVENTTRANSPOSTINGTYPE
        , itp2.isposted                                         AS ISPOSTED
        , itp2.postingtype                                      AS POSTINGTYPE
        , itp2.postingtypeoffset                                AS POSTINGTYPEOFFSET
        , itp2.ledgerdimension                                  AS LEDGERDIMENSION
        , itp2.offsetledgerdimension                            AS LEDGERDIMENSIONOFFSET
        , itp2.dataareaid                                      AS DATAAREAID#4
        , itp2.partition                                        AS PARTITION#4
        , (CAST((- (it.qty)) AS NUMERIC(32, 6)))                AS QTY
        , (CAST((- (it.costamountphysical)) AS NUMERIC(32, 6))) AS AMOUNT
      FROM {{ ref('inventtrans') }}             it
    INNER JOIN {{ ref('inventtransorigin') }}  ito
        ON ito.recid                  = it.inventtransorigin
    -- AND ito.isexcludedfrominventoryvalue = 0
    INNER JOIN {{ ref('inventtransposting') }} itp
        ON itp.dataareaid             = it.dataareaid
      AND itp.voucher                 = it.voucher
      AND itp.transdate               = it.datefinancial
      AND itp.inventtranspostingtype  = 1
      AND itp.inventtransorigin       = ito.recid
    INNER JOIN {{ ref('inventtransposting') }} itp2
        ON itp2.dataareaid            = it.dataareaid
      AND itp2.voucher                = it.voucherphysical
      AND itp2.transdate              = it.datephysical
      AND itp2.inventtranspostingtype = 0
      AND itp2.inventtransorigin      = ito.recid;
),
inventoryvaluebalance_factinventvaluephysicalandfinancialsettlement AS (
    SELECT it.inventdimid                                                   AS INVENTDIMID
        , it.statusissue                                                   AS STATUSISSUE
        , it.statusreceipt                                                 AS STATUSRECEIPT
        , it.markingrefinventtransorigin                                   AS MARKINGREFINVENTTRANSORIGIN
        , it.returninventtransorigin                                       AS RETURNINVENTTRANSORIGIN
        , it.valueopen                                                     AS VALUEOPEN
        , it.inventtransorigin                                             AS INVENTTRANSORIGIN
        , it.voucherphysical                                               AS VOUCHERPHYSICAL
        , it.recid                                                        AS INVENTTRANSRECID
        , it.dataareaid                                                   AS DATAAREAID
        , it.partition                                                     AS PARTITION
        , it.recid                                                        AS RECID
        , ito.inventtransid                                                AS INVENTTRANSID
        , ito.referencecategory                                            AS REFERENCECATEGORY
        , ito.referenceid                                                  AS REFERENCE
        , ito.dataareaid                                                  AS DATAAREAID#2
        , ito.partition                                                    AS PARTITION#2
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
        , ist.dataareaid                                                  AS DATAAREAID#3
        , ist.partition                                                    AS PARTITION#3
        , (CAST((0) AS NUMERIC(32, 6)))                                    AS QTY
        , (CAST((CASE WHEN ist.settlemodel = 7 THEN 0 ELSE 1 END) AS INT)) AS INVENTTRANSPOSTINGTYPE
        , (CAST((CASE WHEN ist.settlemodel = 7 THEN 0 ELSE 1 END) AS INT)) AS UPDATEINVENTTRANSPOSTINGTYPE
      FROM {{ ref('inventtrans') }}            it
    INNER JOIN {{ ref('inventtransorigin') }} ito
        ON ito.recid      = it.inventtransorigin
    --AND ito.isexcludedfrominventoryvalue = 0
    INNER JOIN {{ ref('inventsettlement') }}  ist
        ON ist.dataareaid = it.dataareaid
      AND ist.transrecid  = it.recid;
),
inventoryvaluebalance_factinventvaluetransunionall AS (
    SELECT t.AMOUNT
        , t.TRANSBEGINTIME
        , t.INVENTDIMID
        , t.INVENTTRANSID
        , t.INVENTTRANSPOSTINGTYPE
        , t.ISPOSTED
        , t.ITEMID
        , t.LEDGERDIMENSION
        , t.LEDGERDIMENSIONOFFSET
        , t.POSTINGTYPE
        , t.POSTINGTYPEOFFSET
        , t.QTY
        , t.REFERENCE
        , t.REFERENCECATEGORY
        , t.TRANSDATE
        , t.VOUCHER
        , t.STATUSISSUE
        , t.STATUSRECEIPT
        , t.MARKINGREFINVENTTRANSORIGIN
        , t.RETURNINVENTTRANSORIGIN
        , t.VALUEOPEN
        , t.UPDATEINVENTTRANSPOSTINGTYPE
        , t.INVENTTRANSORIGIN
        , t.VOUCHERPHYSICAL
        , t.INVENTTRANSRECID
        , t.DATAAREAID
        , t.PARTITION
        , t.RECID
        , t.UNIONALLBRANCHID
      FROM (   SELECT T1.AMOUNT                       AS AMOUNT
                    , T1.TRANSBEGINTIME               AS TRANSBEGINTIME
                    , T1.INVENTDIMID                  AS INVENTDIMID
                    , T1.INVENTTRANSID                AS INVENTTRANSID
                    , T1.INVENTTRANSPOSTINGTYPE       AS INVENTTRANSPOSTINGTYPE
                    , T1.ISPOSTED                     AS ISPOSTED
                    , T1.ITEMID                       AS ITEMID
                    , T1.LEDGERDIMENSION              AS LEDGERDIMENSION
                    , T1.LEDGERDIMENSIONOFFSET        AS LEDGERDIMENSIONOFFSET
                    , T1.POSTINGTYPE                  AS POSTINGTYPE
                    , T1.POSTINGTYPEOFFSET            AS POSTINGTYPEOFFSET
                    , T1.QTY                          AS QTY
                    , T1.REFERENCE                    AS REFERENCE
                    , T1.REFERENCECATEGORY            AS REFERENCECATEGORY
                    , T1.TRANSDATE                    AS TRANSDATE
                    , T1.VOUCHER                      AS VOUCHER
                    , T1.STATUSISSUE                  AS STATUSISSUE
                    , T1.STATUSRECEIPT                AS STATUSRECEIPT
                    , T1.MARKINGREFINVENTTRANSORIGIN  AS MARKINGREFINVENTTRANSORIGIN
                    , T1.RETURNINVENTTRANSORIGIN      AS RETURNINVENTTRANSORIGIN
                    , T1.VALUEOPEN                    AS VALUEOPEN
                    , T1.UPDATEINVENTTRANSPOSTINGTYPE AS UPDATEINVENTTRANSPOSTINGTYPE
                    , T1.INVENTTRANSORIGIN            AS INVENTTRANSORIGIN
                    , T1.VOUCHERPHYSICAL              AS VOUCHERPHYSICAL
                    , T1.RECID                        AS INVENTTRANSRECID
                    , T1.DATAAREAID                   AS DATAAREAID
                    , T1.PARTITION                    AS PARTITION
                    , T1.RECID                        AS RECID
                    , 1                               AS UNIONALLBRANCHID
                FROM inventoryvaluebalance_factinventvaluephysicaladjustment T1
              UNION ALL
              SELECT T1.AMOUNT                       AS AMOUNT
                    , T1.TRANSBEGINTIME               AS TRANSBEGINTIME
                    , T1.INVENTDIMID                  AS INVENTDIMID
                    , T1.INVENTTRANSID                AS INVENTTRANSID
                    , T1.INVENTTRANSPOSTINGTYPE       AS INVENTTRANSPOSTINGTYPE
                    , T1.ISPOSTED                     AS ISPOSTED
                    , T1.ITEMID                       AS ITEMID
                    , T1.LEDGERDIMENSION              AS LEDGERDIMENSION
                    , T1.LEDGERDIMENSIONOFFSET        AS LEDGERDIMENSIONOFFSET
                    , T1.POSTINGTYPE                  AS POSTINGTYPE
                    , T1.POSTINGTYPEOFFSET            AS POSTINGTYPEOFFSET
                    , T1.QTY                          AS QTY
                    , T1.REFERENCE                    AS REFERENCE
                    , T1.REFERENCECATEGORY            AS REFERENCECATEGORY
                    , T1.TRANSDATE                    AS TRANSDATE
                    , T1.VOUCHER                      AS VOUCHER
                    , T1.STATUSISSUE                  AS STATUSISSUE
                    , T1.STATUSRECEIPT                AS STATUSRECEIPT
                    , T1.MARKINGREFINVENTTRANSORIGIN  AS MARKINGREFINVENTTRANSORIGIN
                    , T1.RETURNINVENTTRANSORIGIN      AS RETURNINVENTTRANSORIGIN
                    , T1.VALUEOPEN                    AS VALUEOPEN
                    , T1.UPDATEINVENTTRANSPOSTINGTYPE AS UPDATEINVENTTRANSPOSTINGTYPE
                    , T1.INVENTTRANSORIGIN            AS INVENTTRANSORIGIN
                    , T1.VOUCHERPHYSICAL              AS VOUCHERPHYSICAL
                    , T1.RECID                        AS INVENTTRANSRECID
                    , T1.DATAAREAID                   AS DATAAREAID
                    , T1.PARTITION                    AS PARTITION
                    , T1.RECID                        AS RECID
                    , 2                               AS UNIONALLBRANCHID
                FROM inventoryvaluebalance_factinventvaluephysicaltransaction T1
              UNION ALL
              SELECT T1.AMOUNT                       AS AMOUNT
                    , T1.TRANSBEGINTIME               AS TRANSBEGINTIME
                    , T1.INVENTDIMID                  AS INVENTDIMID
                    , T1.INVENTTRANSID                AS INVENTTRANSID
                    , T1.INVENTTRANSPOSTINGTYPE       AS INVENTTRANSPOSTINGTYPE
                    , T1.ISPOSTED                     AS ISPOSTED
                    , T1.ITEMID                       AS ITEMID
                    , T1.LEDGERDIMENSION              AS LEDGERDIMENSION
                    , T1.LEDGERDIMENSIONOFFSET        AS LEDGERDIMENSIONOFFSET
                    , T1.POSTINGTYPE                  AS POSTINGTYPE
                    , T1.POSTINGTYPEOFFSET            AS POSTINGTYPEOFFSET
                    , T1.QTY                          AS QTY
                    , T1.REFERENCE                    AS REFERENCE
                    , T1.REFERENCECATEGORY            AS REFERENCECATEGORY
                    , T1.TRANSDATE                    AS TRANSDATE
                    , T1.VOUCHER                      AS VOUCHER
                    , T1.STATUSISSUE                  AS STATUSISSUE
                    , T1.STATUSRECEIPT                AS STATUSRECEIPT
                    , T1.MARKINGREFINVENTTRANSORIGIN  AS MARKINGREFINVENTTRANSORIGIN
                    , T1.RETURNINVENTTRANSORIGIN      AS RETURNINVENTTRANSORIGIN
                    , T1.VALUEOPEN                    AS VALUEOPEN
                    , T1.UPDATEINVENTTRANSPOSTINGTYPE AS UPDATEINVENTTRANSPOSTINGTYPE
                    , T1.INVENTTRANSORIGIN            AS INVENTTRANSORIGIN
                    , T1.VOUCHERPHYSICAL              AS VOUCHERPHYSICAL
                    , T1.RECID                        AS INVENTTRANSRECID
                    , T1.DATAAREAID                   AS DATAAREAID
                    , T1.PARTITION                    AS PARTITION
                    , T1.RECID                        AS RECID
                    , 3                               AS UNIONALLBRANCHID
                FROM inventoryvaluebalance_factinventvaluefinancialtransaction T1
              UNION ALL
              SELECT T1.AMOUNT                       AS AMOUNT
                    , T1.TRANSBEGINTIME               AS TRANSBEGINTIME
                    , T1.INVENTDIMID                  AS INVENTDIMID
                    , T1.INVENTTRANSID                AS INVENTTRANSID
                    , T1.INVENTTRANSPOSTINGTYPE       AS INVENTTRANSPOSTINGTYPE
                    , T1.ISPOSTED                     AS ISPOSTED
                    , T1.ITEMID                       AS ITEMID
                    , T1.LEDGERDIMENSION              AS LEDGERDIMENSION
                    , T1.LEDGERDIMENSIONOFFSET        AS LEDGERDIMENSIONOFFSET
                    , T1.POSTINGTYPE                  AS POSTINGTYPE
                    , T1.POSTINGTYPEOFFSET            AS POSTINGTYPEOFFSET
                    , T1.QTY                          AS QTY
                    , T1.REFERENCE                    AS REFERENCE
                    , T1.REFERENCECATEGORY            AS REFERENCECATEGORY
                    , T1.TRANSDATE                    AS TRANSDATE
                    , T1.VOUCHER                      AS VOUCHER
                    , T1.STATUSISSUE                  AS STATUSISSUE
                    , T1.STATUSRECEIPT                AS STATUSRECEIPT
                    , T1.MARKINGREFINVENTTRANSORIGIN  AS MARKINGREFINVENTTRANSORIGIN
                    , T1.RETURNINVENTTRANSORIGIN      AS RETURNINVENTTRANSORIGIN
                    , T1.VALUEOPEN                    AS VALUEOPEN
                    , T1.UPDATEINVENTTRANSPOSTINGTYPE AS UPDATEINVENTTRANSPOSTINGTYPE
                    , T1.INVENTTRANSORIGIN            AS INVENTTRANSORIGIN
                    , T1.VOUCHERPHYSICAL              AS VOUCHERPHYSICAL
                    , T1.RECID                        AS INVENTTRANSRECID
                    , T1.DATAAREAID                   AS DATAAREAID
                    , T1.PARTITION                    AS PARTITION
                    , T1.RECID                        AS RECID
                    , 4                               AS UNIONALLBRANCHID
                FROM inventoryvaluebalance_factinventvaluephysicalreversed T1
              UNION ALL
              SELECT T1.AMOUNT                       AS AMOUNT
                    , T1.TRANSBEGINTIME               AS TRANSBEGINTIME
                    , T1.INVENTDIMID                  AS INVENTDIMID
                    , T1.INVENTTRANSID                AS INVENTTRANSID
                    , T1.INVENTTRANSPOSTINGTYPE       AS INVENTTRANSPOSTINGTYPE
                    , T1.ISPOSTED                     AS ISPOSTED
                    , T1.ITEMID                       AS ITEMID
                    , T1.LEDGERDIMENSION              AS LEDGERDIMENSION
                    , T1.LEDGERDIMENSIONOFFSET        AS LEDGERDIMENSIONOFFSET
                    , T1.POSTINGTYPE                  AS POSTINGTYPE
                    , T1.POSTINGTYPEOFFSET            AS POSTINGTYPEOFFSET
                    , T1.QTY                          AS QTY
                    , T1.REFERENCE                    AS REFERENCE
                    , T1.REFERENCECATEGORY            AS REFERENCECATEGORY
                    , T1.TRANSDATE                    AS TRANSDATE
                    , T1.VOUCHER                      AS VOUCHER
                    , T1.STATUSISSUE                  AS STATUSISSUE
                    , T1.STATUSRECEIPT                AS STATUSRECEIPT
                    , T1.MARKINGREFINVENTTRANSORIGIN  AS MARKINGREFINVENTTRANSORIGIN
                    , T1.RETURNINVENTTRANSORIGIN      AS RETURNINVENTTRANSORIGIN
                    , T1.VALUEOPEN                    AS VALUEOPEN
                    , T1.UPDATEINVENTTRANSPOSTINGTYPE AS UPDATEINVENTTRANSPOSTINGTYPE
                    , T1.INVENTTRANSORIGIN            AS INVENTTRANSORIGIN
                    , T1.VOUCHERPHYSICAL              AS VOUCHERPHYSICAL
                    , T1.RECID                        AS INVENTTRANSRECID
                    , T1.DATAAREAID                   AS DATAAREAID
                    , T1.PARTITION                    AS PARTITION
                    , T1.RECID                        AS RECID
                    , 5                               AS UNIONALLBRANCHID
                FROM inventoryvaluebalance_factinventvaluephysicalandfinancialsettlement T1) t;
),
inventoryvaluebalance_factinventvaluereportview AS (
    SELECT t.AMOUNT
        , t.INVENTDIMID
        , t.INVENTTRANSID
        , t.INVENTTRANSPOSTINGTYPE
        , t.ISPOSTED
        , t.ITEMID
        , t.LEDGERDIMENSION
        , t.LEDGERDIMENSIONOFFSET
        , t.POSTINGTYPE
        , t.POSTINGTYPEOFFSET
        , t.QTY
        , t.REFERENCE
        , t.REFERENCECATEGORY
        , t.TRANSDATE
        , t.VOUCHER
        , t.TRANSBEGINTIME
        , t.DATAAREAID
        , t.PARTITION
        , t.RECID
        , t.UNIONALLBRANCHID
        , t.STATEMENTLINECATEGORYLEVEL2
        , t.STATEMENTTYPE
        , t.STATEMENTLINECATEGORYLEVEL3
        , t.STATEMENTLINECATEGORYLEVEL1
      FROM (   SELECT T1.AMOUNT                                                         AS AMOUNT
                    , T1.INVENTDIMID                                                    AS INVENTDIMID
                    , T1.INVENTTRANSID                                                  AS INVENTTRANSID
                    , T1.INVENTTRANSPOSTINGTYPE                                         AS INVENTTRANSPOSTINGTYPE
                    , T1.ISPOSTED                                                       AS ISPOSTED
                    , T1.ITEMID                                                         AS ITEMID
                    , T1.LEDGERDIMENSION                                                AS LEDGERDIMENSION
                    , T1.LEDGERDIMENSIONOFFSET                                          AS LEDGERDIMENSIONOFFSET
                    , T1.POSTINGTYPE                                                    AS POSTINGTYPE
                    , T1.POSTINGTYPEOFFSET                                              AS POSTINGTYPEOFFSET
                    , T1.QTY                                                            AS QTY
                    , T1.REFERENCE                                                      AS REFERENCE
                    , T1.REFERENCECATEGORY                                              AS REFERENCECATEGORY
                    , T1.TRANSDATE                                                      AS TRANSDATE
                    , T1.VOUCHER                                                        AS VOUCHER
                    , T1.TRANSBEGINTIME                                                 AS TRANSBEGINTIME
                    , T1.DATAAREAID                                                     AS DATAAREAID
                    , T1.PARTITION                                                      AS PARTITION
                    , T1.RECID                                                          AS RECID
                    , 1                                                                 AS UNIONALLBRANCHID
                    , (CAST((CASE WHEN ISPOSTED = 1
                                  THEN CASE POSTINGTYPEOFFSET
                                            WHEN 106
                                            THEN 2
                                            WHEN 108
                                            THEN 4
                                            WHEN 110
                                            THEN 2
                                            WHEN 111
                                            THEN 4
                                            WHEN 121
                                            THEN 4
                                            WHEN 219
                                            THEN 5
                                            WHEN 220
                                            THEN 5
                                            WHEN 52
                                            THEN 6
                                            WHEN 60
                                            THEN 6
                                            WHEN 71
                                            THEN 1
                                            WHEN 82
                                            THEN 1
                                            WHEN 83
                                            THEN 1
                                            ELSE 3 END
                                  ELSE CASE REFERENCECATEGORY
                                            WHEN 0
                                            THEN 6
                                            WHEN 100
                                            THEN 2
                                            WHEN 14
                                            THEN 5
                                            WHEN 15
                                            THEN 5
                                            WHEN 2
                                            THEN 2
                                            WHEN 201
                                            THEN 5
                                            WHEN 202
                                            THEN 5
                                            WHEN 203
                                            THEN 5
                                            WHEN 21
                                            THEN 5
                                            WHEN 22
                                            THEN 5
                                            WHEN 27
                                            THEN 2
                                            WHEN 28
                                            THEN 5
                                            WHEN 29
                                            THEN 5
                                            WHEN 3
                                            THEN 1
                                            WHEN 30
                                            THEN 4
                                            WHEN 31
                                            THEN 4
                                            WHEN 32
                                            THEN 4
                                            WHEN 33
                                            THEN 0
                                            WHEN 34
                                            THEN 0
                                            WHEN 6
                                            THEN 5
                                            WHEN 7
                                            THEN 5
                                            WHEN 8
                                            THEN 4
                                            ELSE 3 END END) AS INT))                    AS STATEMENTLINECATEGORYLEVEL2
                    , (CAST((1) AS INT))                                                AS STATEMENTTYPE
                    , (CAST((CASE REFERENCECATEGORY WHEN 13 THEN 1 ELSE 0 END) AS INT)) AS STATEMENTLINECATEGORYLEVEL3
                    , (CAST((CASE REFERENCECATEGORY
                                  WHEN 100
                                  THEN 1
                                  WHEN 2
                                  THEN 1
                                  WHEN 27
                                  THEN 1
                                  WHEN 3
                                  THEN 1
                                  WHEN 33
                                  THEN 0
                                  WHEN 34
                                  THEN 0
                                  ELSE 2 END) AS INT))                                  AS STATEMENTLINECATEGORYLEVEL1
                FROM inventoryvaluebalance_factinventvaluefinancialbalance T1
              UNION ALL
              SELECT T1.AMOUNT                                                         AS AMOUNT
                    , T1.INVENTDIMID                                                    AS INVENTDIMID
                    , T1.INVENTTRANSID                                                  AS INVENTTRANSID
                    , T1.INVENTTRANSPOSTINGTYPE                                         AS INVENTTRANSPOSTINGTYPE
                    , T1.ISPOSTED                                                       AS ISPOSTED
                    , T1.ITEMID                                                         AS ITEMID
                    , T1.LEDGERDIMENSION                                                AS LEDGERDIMENSION
                    , T1.LEDGERDIMENSIONOFFSET                                          AS LEDGERDIMENSIONOFFSET
                    , T1.POSTINGTYPE                                                    AS POSTINGTYPE
                    , T1.POSTINGTYPEOFFSET                                              AS POSTINGTYPEOFFSET
                    , T1.QTY                                                            AS QTY
                    , T1.REFERENCE                                                      AS REFERENCE
                    , T1.REFERENCECATEGORY                                              AS REFERENCECATEGORY
                    , T1.TRANSDATE                                                      AS TRANSDATE
                    , T1.VOUCHER                                                        AS VOUCHER
                    , T1.TRANSBEGINTIME                                                 AS TRANSBEGINTIME
                    , T1.DATAAREAID                                                     AS DATAAREAID
                    , T1.PARTITION                                                      AS PARTITION
                    , T1.RECID                                                          AS RECID
                    , 2                                                                 AS UNIONALLBRANCHID
                    , (CAST((CASE WHEN ISPOSTED = 1
                                  THEN CASE POSTINGTYPEOFFSET
                                            WHEN 106
                                            THEN 2
                                            WHEN 108
                                            THEN 4
                                            WHEN 110
                                            THEN 2
                                            WHEN 111
                                            THEN 4
                                            WHEN 121
                                            THEN 4
                                            WHEN 219
                                            THEN 5
                                            WHEN 220
                                            THEN 5
                                            WHEN 52
                                            THEN 6
                                            WHEN 60
                                            THEN 6
                                            WHEN 71
                                            THEN 1
                                            WHEN 82
                                            THEN 1
                                            WHEN 83
                                            THEN 1
                                            ELSE 3 END
                                  ELSE CASE REFERENCECATEGORY
                                            WHEN 0
                                            THEN 6
                                            WHEN 100
                                            THEN 2
                                            WHEN 14
                                            THEN 5
                                            WHEN 15
                                            THEN 5
                                            WHEN 2
                                            THEN 2
                                            WHEN 201
                                            THEN 5
                                            WHEN 202
                                            THEN 5
                                            WHEN 203
                                            THEN 5
                                            WHEN 21
                                            THEN 5
                                            WHEN 22
                                            THEN 5
                                            WHEN 27
                                            THEN 2
                                            WHEN 28
                                            THEN 5
                                            WHEN 29
                                            THEN 5
                                            WHEN 3
                                            THEN 1
                                            WHEN 30
                                            THEN 4
                                            WHEN 31
                                            THEN 4
                                            WHEN 32
                                            THEN 4
                                            WHEN 33
                                            THEN 0
                                            WHEN 34
                                            THEN 0
                                            WHEN 6
                                            THEN 5
                                            WHEN 7
                                            THEN 5
                                            WHEN 8
                                            THEN 4
                                            ELSE 3 END END) AS INT))                    AS STATEMENTLINECATEGORYLEVEL2
                    , (CAST((1) AS INT))                                                AS STATEMENTTYPE
                    , (CAST((CASE REFERENCECATEGORY WHEN 13 THEN 1 ELSE 0 END) AS INT)) AS STATEMENTLINECATEGORYLEVEL3
                    , (CAST((CASE REFERENCECATEGORY
                                  WHEN 100
                                  THEN 1
                                  WHEN 2
                                  THEN 1
                                  WHEN 27
                                  THEN 1
                                  WHEN 3
                                  THEN 1
                                  WHEN 33
                                  THEN 0
                                  WHEN 34
                                  THEN 0
                                  ELSE 2 END) AS INT))                                  AS STATEMENTLINECATEGORYLEVEL1
                FROM inventoryvaluebalance_factinventvaluephysicalbalance T1
              UNION ALL
              SELECT T1.AMOUNT                                                         AS AMOUNT
                    , T1.INVENTDIMID                                                    AS INVENTDIMID
                    , T1.INVENTTRANSID                                                  AS INVENTTRANSID
                    , T1.INVENTTRANSPOSTINGTYPE                                         AS INVENTTRANSPOSTINGTYPE
                    , T1.ISPOSTED                                                       AS ISPOSTED
                    , T1.ITEMID                                                         AS ITEMID
                    , T1.LEDGERDIMENSION                                                AS LEDGERDIMENSION
                    , T1.LEDGERDIMENSIONOFFSET                                          AS LEDGERDIMENSIONOFFSET
                    , T1.POSTINGTYPE                                                    AS POSTINGTYPE
                    , T1.POSTINGTYPEOFFSET                                              AS POSTINGTYPEOFFSET
                    , T1.QTY                                                            AS QTY
                    , T1.REFERENCE                                                      AS REFERENCE
                    , T1.REFERENCECATEGORY                                              AS REFERENCECATEGORY
                    , T1.TRANSDATE                                                      AS TRANSDATE
                    , T1.VOUCHER                                                        AS VOUCHER
                    , T1.TRANSBEGINTIME                                                 AS TRANSBEGINTIME
                    , T1.DATAAREAID                                                     AS DATAAREAID
                    , T1.PARTITION                                                      AS PARTITION
                    , T1.RECID                                                          AS RECID
                    , 3                                                                 AS UNIONALLBRANCHID
                    , (CAST((CASE WHEN ISPOSTED = 1
                                  THEN CASE POSTINGTYPEOFFSET
                                            WHEN 106
                                            THEN 2
                                            WHEN 108
                                            THEN 4
                                            WHEN 110
                                            THEN 2
                                            WHEN 111
                                            THEN 4
                                            WHEN 121
                                            THEN 4
                                            WHEN 219
                                            THEN 5
                                            WHEN 220
                                            THEN 5
                                            WHEN 52
                                            THEN 6
                                            WHEN 60
                                            THEN 6
                                            WHEN 71
                                            THEN 1
                                            WHEN 82
                                            THEN 1
                                            WHEN 83
                                            THEN 1
                                            ELSE 3 END
                                  ELSE CASE REFERENCECATEGORY
                                            WHEN 0
                                            THEN 6
                                            WHEN 100
                                            THEN 2
                                            WHEN 14
                                            THEN 5
                                            WHEN 15
                                            THEN 5
                                            WHEN 2
                                            THEN 2
                                            WHEN 201
                                            THEN 5
                                            WHEN 202
                                            THEN 5
                                            WHEN 203
                                            THEN 5
                                            WHEN 21
                                            THEN 5
                                            WHEN 22
                                            THEN 5
                                            WHEN 27
                                            THEN 2
                                            WHEN 28
                                            THEN 5
                                            WHEN 29
                                            THEN 5
                                            WHEN 3
                                            THEN 1
                                            WHEN 30
                                            THEN 4
                                            WHEN 31
                                            THEN 4
                                            WHEN 32
                                            THEN 4
                                            WHEN 33
                                            THEN 0
                                            WHEN 34
                                            THEN 0
                                            WHEN 6
                                            THEN 5
                                            WHEN 7
                                            THEN 5
                                            WHEN 8
                                            THEN 4
                                            ELSE 3 END END) AS INT))                    AS STATEMENTLINECATEGORYLEVEL2
                    , (CAST((1) AS INT))                                                AS STATEMENTTYPE
                    , (CAST((CASE REFERENCECATEGORY WHEN 13 THEN 1 ELSE 0 END) AS INT)) AS STATEMENTLINECATEGORYLEVEL3
                    , (CAST((CASE REFERENCECATEGORY
                                  WHEN 100
                                  THEN 1
                                  WHEN 2
                                  THEN 1
                                  WHEN 27
                                  THEN 1
                                  WHEN 3
                                  THEN 1
                                  WHEN 33
                                  THEN 0
                                  WHEN 34
                                  THEN 0
                                  ELSE 2 END) AS INT))                                  AS STATEMENTLINECATEGORYLEVEL1
                FROM inventoryvaluebalance_factinventvaluetransunionall T1) t;
),
inventoryvaluebalance_factinventvalueinterimtable AS (
    SELECT ivr.QTY
        , ivr.AMOUNT
        , DATAAREAID
        , INVENTDIMID
        , ITEMID
        , dd.EndDate AS EndDate
      FROM inventoryvaluebalance_factinventvaluereportview ivr
    INNER JOIN inventoryvaluebalance_factdate             dd
        ON ivr.TRANSDATE > dd.EndDate
      AND ivr.TRANSDATE <= { TS '2154-12-31 00:00:00.000' };
),
inventoryvaluebalance_factinventvalue AS (
    select it.itemid
        , sum(ivr.qty)    as qty
        , sum(ivr.amount) as amt
        , id.configid
        , id.inventsizeid
        , id.inventcolorid
        , id.inventstyleid
        , id.inventsiteid
        , id.inventlocationid
        , id.wmslocationid
        , id.licenseplateid
        , id.inventbatchid
        , ivr.enddate
        , it.dataareaid
      from inventoryvaluebalance_factinventvalueinterimtable ivr
    inner join (SELECT itemid, dataareaid  FROM inventoryvaluebalance_factinventtable WHERE itemtype  = 0 )      it
        on ivr.dataareaid  = it.dataareaid
      and it.itemid       = ivr.itemid
    inner join inventoryvaluebalance_factinventdim        id
        on id.dataareaid  = ivr.dataareaid
      and ivr.inventdimid = id.inventdimid
    group by it.itemid
            , id.configid
            , id.inventsizeid
            , id.inventcolorid
            , id.inventstyleid
            , id.inventsiteid
            , id.inventlocationid
            , id.wmslocationid
            , id.licenseplateid
            , id.inventbatchid
            , ivr.enddate
            , it.dataareaid;
),
inventoryvaluebalance_factstage1 AS (
    SELECT iv.enddate         AS AccountingMonth
        , MAX(iar.TRANSDATE) AS AgeDate
        , MAX(iv.qty) * -1   AS InventoryQuantity
        , MAX(iv.amt) * -1   AS InventoryAmount
        , iv.configid
        , iv.inventsizeid
        , iv.inventcolorid
        , iv.inventstyleid
        , iv.inventsiteid    AS SiteID
        , iv.inventlocationid
        , iv.itemid
        , iv.inventbatchid
        , iv.licenseplateid
        , iv.wmslocationid
        , iv.dataareaid     AS LegalEntityID
      FROM inventoryvaluebalance_factinventvalue                 iv
      LEFT JOIN inventoryvaluebalance_factinventagingreceiptview iar
        ON iv.configid         = iar.CONFIGID
      AND iv.inventbatchid    = iar.INVENTBATCHID
      AND iv.inventcolorid    = iar.INVENTCOLORID
      AND iv.inventsiteid     = iar.INVENTSITEID
      AND iv.inventlocationid = iar.INVENTLOCATIONID
      AND iv.inventsizeid     = iar.INVENTSIZEID
      AND iv.inventstyleid    = iar.INVENTSTYLEID
      AND iv.itemid           = iar.ITEMID
      AND iv.licenseplateid   = iar.LICENSEPLATEID
      AND iv.wmslocationid    = iar.WMSLOCATIONID
      AND iar.DATAAREAID      = iv.dataareaid
      AND iar.TRANSDATE       <= iv.enddate
    GROUP BY iv.configid
            , iv.inventsizeid
            , iv.inventcolorid
            , iv.inventstyleid
            , iv.inventsiteid
            , iv.inventlocationid
            , iv.itemid
            , iv.inventbatchid
            , iv.licenseplateid
            , iv.wmslocationid
            , iv.enddate
            , iv.dataareaid;
),
inventoryvaluebalance_factstage AS (
    SELECT ts.AccountingMonth
        , CASE WHEN ts.AgeDate IS NULL
                AND ts.InventoryQuantity < 0
                THEN DATEADD(DAY, -1, ts.AccountingMonth)
                WHEN ts.AgeDate IS NULL
                AND ts.InventoryQuantity > 0
                THEN DATEADD(DAY, -125, ts.AccountingMonth)
                ELSE ts.AgeDate END AS AgeDate
        , ts.InventoryQuantity
        , ts.InventoryAmount
        , ts.SiteID
        , ts.LegalEntityID
        , ts.CONFIGID
        , ts.INVENTSIZEID
        , ts.INVENTCOLORID
        , ts.INVENTSTYLEID
        , ts.INVENTLOCATIONID
        , ts.ITEMID
        , ts.INVENTBATCHID
        , ts.LICENSEPLATEID
        , ts.WMSLOCATIONID
      FROM inventoryvaluebalance_factstage1 ts;
),
inventoryvaluebalance_factdate445 AS (
    SELECT dd1.FiscalDayOfMonthID
        , dd1.FiscalYearDate
        , dd1.Date
        , dd.StartDate
        , dd.EndDate
      FROM {{ ref('date_d') }}   dd1
    INNER JOIN inventoryvaluebalance_factdate dd
        ON dd.FiscalMonthDate = dd1.FiscalMonthDate
      WHERE dd1.Date <= GETDATE ();
),
inventoryvaluebalance_factactivitymonths AS (
    SELECT DISTINCT
          d.EndDate                                                                       AS AccountingMonth
        , d.FiscalYearDate                                                                AS AccountingYear
        , ta.LegalEntityID
        , ta.INVENTSIZEID
        , ta.INVENTCOLORID
        , ta.INVENTSTYLEID
        , ta.INVENTLOCATIONID
        , ta.WMSLOCATIONID
        , ta.CONFIGID
        , ta.ITEMID
        , ta.INVENTBATCHID
        , ta.LICENSEPLATEID
        , ta.SiteID                                                                       AS SiteID
        , CAST(CASE WHEN d.EndDate > GETDATE() THEN GETDATE() ELSE d.EndDate END AS DATE) AS EOMONTHDate
      FROM inventoryvaluebalance_factstage        ta
    INNER JOIN (SELECT * FROM inventoryvaluebalance_factdate445 d
        WHERE d.FiscalDayOfMonthID = 1 AND d.EndDate            <= DATEADD(MONTH, 1, GETDATE())) d
        ON  d.EndDate            >= ta.AccountingMonth
),
inventoryvaluebalance_facttotals AS (
    SELECT tm.AccountingMonth
        , ta.AgeDate
        , ISNULL(InventoryQuantity, 0) AS InventoryQuantity
        , ISNULL(InventoryAmount, 0)   AS InventoryAmount
        , tm.SiteID
        , tm.LegalEntityID
        , tm.CONFIGID
        , tm.INVENTSIZEID
        , tm.INVENTCOLORID
        , tm.INVENTSTYLEID
        , tm.INVENTLOCATIONID
        , tm.ITEMID
        , tm.INVENTBATCHID
        , tm.LICENSEPLATEID
        , tm.WMSLOCATIONID
      FROM inventoryvaluebalance_factactivitymonths tm
      LEFT JOIN inventoryvaluebalance_factstage     ta
        ON ta.LegalEntityID    = tm.LegalEntityID
      AND ta.AccountingMonth  = tm.AccountingMonth
      AND ta.INVENTSIZEID     = tm.INVENTSIZEID
      AND ta.INVENTCOLORID    = tm.INVENTCOLORID
      AND ta.INVENTSTYLEID    = tm.INVENTSTYLEID
      AND ta.INVENTLOCATIONID = tm.INVENTLOCATIONID
      AND ta.WMSLOCATIONID    = tm.WMSLOCATIONID
      AND ta.CONFIGID         = tm.CONFIGID
      AND ta.ITEMID           = tm.ITEMID
      AND ta.SiteID           = tm.SiteID
      AND ta.LICENSEPLATEID   = tm.LICENSEPLATEID
      AND ta.INVENTBATCHID    = tm.INVENTBATCHID
),
inventoryvaluebalance_factopeningbalance AS (
    SELECT CASE WHEN t.AccountingMonth > GETDATE() THEN CAST(GETDATE() AS DATE)ELSE t.AccountingMonth END AS AccountingMonth
        , t.AgeDate
        , t.InventoryQuantity
        , t.InventoryAmount
        , t.SiteID
        , t.LegalEntityID
        , t.CONFIGID
        , t.INVENTSIZEID
        , t.INVENTCOLORID
        , t.INVENTSTYLEID
        , t.INVENTLOCATIONID
        , t.ITEMID
        , t.INVENTBATCHID
        , t.LICENSEPLATEID
        , t.WMSLOCATIONID
        , ISNULL(LAG(ISNULL(t.InventoryAmount, 0)) OVER (PARTITION BY t.LegalEntityID
                                                                    , t.INVENTSIZEID
                                                                    , t.INVENTCOLORID
                                                                    , t.INVENTSTYLEID
                                                                    , t.CONFIGID
                                                                    , t.INVENTLOCATIONID
                                                                    , t.WMSLOCATIONID
                                                                    , t.ITEMID
                                                                    , t.SiteID
                                                                    , t.INVENTBATCHID
                                                                    , t.LICENSEPLATEID
                                                              ORDER BY t.AccountingMonth)
                , 0)                                                                                      AS OpeningValue
        , ISNULL(LAG(ISNULL(t.InventoryQuantity, 0)) OVER (PARTITION BY t.LegalEntityID
                                                                      , t.INVENTSIZEID
                                                                      , t.INVENTCOLORID
                                                                      , t.INVENTSTYLEID
                                                                      , t.CONFIGID
                                                                      , t.INVENTLOCATIONID
                                                                      , t.WMSLOCATIONID
                                                                      , t.ITEMID
                                                                      , t.SiteID
                                                                      , t.INVENTBATCHID
                                                                      , t.LICENSEPLATEID
                                                                ORDER BY t.AccountingMonth)
                , 0)                                                                                      AS OpeningQuantity
        , DATEDIFF(DAY, t.AgeDate, t.AccountingMonth)                                                    AS DaysInInventory
      FROM inventoryvaluebalance_facttotals t
    ORDER BY t.AccountingMonth;
),
inventoryvaluebalance_factdetail1 AS (
    SELECT ISNULL(dp.ProductKey, -1)           AS ProductKey
        , ISNULL(ab.AgingBucketKey, -1)       AS AgingBucketKey
        , ISNULL(dd.DateKey, -1)              AS BalanceDateKey
        , ISNULL(le.LegalEntityKey, -1)       AS LegalEntityKey
        , ISNULL(ds.InventorySiteKey, -1)     AS InventorySiteKey
        , ISNULL(dw.WarehouseKey, -1)         AS WarehouseKey
        , ISNULL(wl.WarehouseLocationKey, -1) AS WarehouseLocationKey
        , SUM(ts.OpeningValue)                AS OpeningValue
        , SUM(ts.OpeningQuantity)             AS OpeningQuantity
        , SUM(ts.InventoryAmount)             AS ClosingValue
        , SUM(ts.InventoryQuantity)           AS ClosingQuantity
        , dp.InventoryUOM                     AS InventoryUOM
        , 1                                   AS _SourceID
      FROM inventoryvaluebalance_factopeningbalance            ts
    INNER JOIN {{ ref('legalentity_d') }}       le
        ON le.LegalEntityID     = ts.LegalEntityID
    INNER JOIN {{ ref('date_d') }}              dd
        ON dd.Date              = ts.AccountingMonth
    INNER JOIN {{ ref('product_d') }}           dp
        ON dp.LegalEntityID     = ts.LegalEntityID
      AND dp.ItemID            = ts.ITEMID
      AND dp.ProductWidth      = ts.INVENTSIZEID
      AND dp.ProductLength     = ts.INVENTCOLORID
      AND dp.ProductColor      = ts.INVENTSTYLEID
      AND dp.ProductConfig     = ts.CONFIGID
      AND dp._SourceID         = 1
      LEFT JOIN {{ ref('warehouse_d') }}         dw
        ON dw.LegalEntityID     = ts.LegalEntityID
      AND dw.WarehouseID       = ts.INVENTLOCATIONID
      LEFT JOIN {{ ref('warehouselocation_d') }} wl
        ON wl.LegalEntityID     = ts.LegalEntityID
      AND wl.WarehouseID       = ts.INVENTLOCATIONID
      AND wl.WarehouseLocation = ts.WMSLOCATIONID
      LEFT JOIN {{ ref('inventorysite_d') }}     ds
        ON ds.LegalEntityID     = ts.LegalEntityID
      AND ds.InventorySiteID   = ts.SiteID
      LEFT JOIN {{ ref('agingbucket_d') }}       ab
        ON ts.DaysInInventory BETWEEN ab.AgeDaysBegin AND AgeDaysEnd
    GROUP BY dp.ProductKey
            , dd.DateKey
            , le.LegalEntityKey
            , InventorySiteKey
            , WarehouseKey
            , WarehouseLocationKey
            , dp.InventoryUOM
            , ab.AgingBucketKey
    HAVING SUM(ts.OpeningValue)      <> 0
        OR SUM(ts.OpeningQuantity)   <> 0
        OR SUM(ts.InventoryAmount)   <> 0
        OR SUM(ts.InventoryQuantity) <> 0;
),
inventoryvaluebalance_factuomcovnversion AS (
    SELECT td.ProductKey									 AS ProductKey
          , td.BalanceDateKey								 AS BalanceDateKey
          , td.AgingBucketKey								 AS AgingBucketKey
          , td.LegalEntityKey								 AS LegalEntityKey
          , td.InventorySiteKey								 AS InventorySiteKey
          , td.WarehouseKey									 AS WarehouseKey
          , td.WarehouseLocationKey							 AS WarehouseLocationKey
          , SUM (td.OpeningQuantity * ISNULL(vuc3.factor, 0)) AS OpeningQuantity_LB
          , SUM (td.ClosingQuantity * ISNULL(vuc3.factor, 0)) AS ClosingQuantity_LB
          , SUM (td.OpeningQuantity * ISNULL(vuc1.factor, 0)) AS OpeningQuantity_PC
          , SUM (td.ClosingQuantity * ISNULL(vuc1.factor, 0)) AS ClosingQuantity_PC
          -- , SUM (td.OpeningQuantity * ISNULL(vuc3.factor, 0) * 0.0005 ) AS OpeningQuantity_TON
          -- , SUM (td.ClosingQuantity * ISNULL(vuc3.factor, 0) * 0.0005 ) AS ClosingQuantity_TON
        FROM inventoryvaluebalance_factdetail1               td
        LEFT JOIN {{ ref('vwuomconversion_pc') }} vuc1
          ON vuc1.legalentitykey = td.LegalEntityKey
        AND vuc1.productkey     = td.ProductKey
        AND vuc1.fromuom        = td.InventoryUOM
    --  AND vuc1.touom          = 'pc'
        LEFT JOIN {{ ref('vwuomconversion_lb') }} vuc3
          ON vuc3.legalentitykey = td.LegalEntityKey
        AND vuc3.productkey     = td.ProductKey
        AND vuc3.fromuom        = td.InventoryUOM
    --  AND vuc3.touom          = 'LB'
      GROUP BY td.ProductKey
              , td.BalanceDateKey
              , td.LegalEntityKey
              , td.AgingBucketKey
              , InventorySiteKey
              , WarehouseKey
              , WarehouseLocationKey;
)
SELECT {{ dbt_utils.generate_surrogate_key(['td._SourceID']) }} AS InventoryValueBalanceKey
    , td.ProductKey                                   AS ProductKey
    , td.BalanceDateKey                               AS BalanceDateKey
    , td.AgingBucketKey                               AS AgingBucketKey
    , td.LegalEntityKey                               AS LegalEntityKey
    , td.InventorySiteKey                             AS InventorySiteKey
    , td.WarehouseKey                                 AS WarehouseKey
    , td.WarehouseLocationKey                         AS WarehouseLocationKey
    , td.OpeningValue                                 AS OpeningValue
    , td.OpeningQuantity                              AS OpeningQuantity
    , uc.OpeningQuantity_LB                           AS OpeningQuantity_LB
    , td.ClosingValue                                 AS ClosingValue
    , td.ClosingQuantity                              AS ClosingQuantity
    , uc.ClosingQuantity_LB                           AS ClosingQuantity_LB
    , td.ClosingValue - td.OpeningValue               AS TransValue
    , td.ClosingQuantity - td.OpeningQuantity         AS TransQuantity
    , uc.ClosingQuantity_LB - uc.OpeningQuantity_LB   AS TransQuantity_LB
    -- , uc.OpeningQuantity_TON                          AS OpeningQuantity_TON
    -- , uc.ClosingQuantity_TON                          AS ClosingQuantity_TON
    -- , uc.ClosingQuantity_TON - uc.OpeningQuantity_TON AS TransQuantity_TON
    , uc.OpeningQuantity_PC                           AS OpeningQuantity_PC
    , uc.ClosingQuantity_PC                           AS ClosingQuantity_PC
    , uc.ClosingQuantity_PC - uc.OpeningQuantity_PC   AS TransQuantity_PC
    , td._SourceID                                    AS _SourceID
    , cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate
  FROM inventoryvaluebalance_factdetail1            td
  LEFT JOIN inventoryvaluebalance_factuomcovnversion uc
    ON uc.ProductKey           = td.ProductKey
  AND uc.BalanceDateKey       = td.BalanceDateKey
  AND uc.LegalEntityKey       = td.LegalEntityKey
  AND uc.AgingBucketKey       = td.AgingBucketKey
  AND uc.InventorySiteKey     = td.InventorySiteKey
  AND uc.WarehouseKey         = td.WarehouseKey
  AND uc.WarehouseLocationKey = td.WarehouseLocationKey;
