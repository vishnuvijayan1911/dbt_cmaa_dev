{{ config(materialized='table', tags=['silver'], alias='tagcostgroup_fact') }}

-- Source file: cma/cma/layers/_base/_silver/tagcostgroup_f/tagcostgroup_f.py
-- Root method: TagCostGroupFact.Tagcostgroupfactdetail [TagCostGroup_FactDetail]
-- Inlined methods: TagCostGroupFact.Tagcostgroupfactstage [TagCostGroup_FactStage], TagCostGroupFact.Tagcostgroupfactdetailmain [TagCostGroup_FactDetailMain], TagCostGroupFact.Tagcostgroupfactdetail1 [TagCostGroup_FactDetail1]
-- external_table_name: TagCostGroup_FactDetail
-- schema_name: temp

WITH
tagcostgroup_factstage AS (
    SELECT DISTINCT
           ib.inventbatchid
         , ib.itemid
         , avg(ib.inventqty)  as inventqty
         , avg(ib.costamount) as costamount
         , ib.costgroupid
         , min(ib.recid) as _recid
         , ib.dataareaid
         , id.configid
         , id.inventcolorid
         , id.inventstyleid
         , id.inventsizeid
         , it.cmacostingunit
         , 1 as _sourceid
      FROM {{ ref('inventsum') }}                   oi
     INNER JOIN {{ ref('inventdim') }}              id
        ON id.dataareaid    = oi.dataareaid
       AND id.inventdimid   = oi.inventdimid
     INNER JOIN {{ ref('tagcostsbycostgroup') }} ib
        ON ib.dataareaid    = id.dataareaid
       AND ib.itemid        = oi.itemid
       AND ib.inventbatchid = id.inventbatchid
       AND ib.inventbatchid <> ''
      LEFT JOIN {{ ref('inventtable') }}            it
        ON it.dataareaid    = id.dataareaid
       AND it.itemid        = oi.itemid
     WHERE financial = 1
       AND oi.closed = 0
     GROUP BY ib.inventbatchid
    ,ib.itemid
    ,ib.costgroupid
    ,ib.dataareaid
    ,id.configid
    ,id.inventcolorid
    ,id.inventstyleid
    ,id.inventsizeid
    ,it.cmacostingunit;
),
tagcostgroup_factdetailmain AS (
    SELECT 
          le.legalentitykey
        , t.tagkey
        , dp.productkey
        , cg.costgroupkey
        , u2.uomkey                                    AS cmacostingunitkey
        , ac.costamount
        , ac.costgroupid
        , ac.inventqty
        , ac.inventqty * ISNULL (vuc.factor, 1) * 0.01 AS costingqty
        , ac._recid
        , ac._sourceid
     FROM tagcostgroup_factstage            ac
    INNER JOIN {{ ref('legalentity_d') }}     le
       ON le.legalentityid   = ac.dataareaid
     LEFT JOIN {{ ref('product_d') }}         dp
       ON dp.legalentityid   = ac.dataareaid
      AND dp.itemid          = ac.itemid
      AND dp.productwidth    = ac.inventsizeid
      AND dp.productlength   = ac.inventcolorid
      AND dp.productcolor    = ac.inventstyleid
      AND dp.productconfig   = ac.configid
      AND dp._sourceid       = 1
     LEFT JOIN {{ ref('uom_d') }}             u1
       ON lower(u1.uom)      = lower(dp.inventoryuom)
     LEFT JOIN {{ ref('uom_d') }}             u2
       ON lower(u2.uom)      = lower(ac.cmacostingunit)
     LEFT JOIN {{ ref('vwuomconversion_lb') }} vuc
       ON vuc.productkey     = dp.productkey
      AND vuc.legalentitykey = le.legalentitykey
      AND vuc.fromuomkey     = u1.uomkey
     -- AND lower(vuc.touom)  = 'lb'
     LEFT JOIN {{ ref('tag_d') }}             t
       ON t.tagid            = ac.inventbatchid
      AND t.itemid           = ac.itemid
      AND t.legalentityid    = ac.dataareaid
     AND t._sourceid       = 1
     LEFT JOIN {{ ref('costgroup_d') }}       cg
       ON cg.costgroupid     = ac.costgroupid;
),
tagcostgroup_factdetail1 AS (
    SELECT 
         d.legalentitykey
       , d.tagkey
       , d.productkey
       , d.costgroupkey
       , d.cmacostingunitkey
       , d.costgroupid
       , d.inventqty
       , d.costingqty
       , d.costamount
       , d.costamount/isnull(nullif(d.costingqty, 0), 1) as unitcost
       , cast(CURRENT_TIMESTAMP as DATETIME2(6))                               as _createddate
       , cast(CURRENT_TIMESTAMP as DATETIME2(6))                               as _modifieddate 
       , d._sourceid
       , d._recid
    FROM tagcostgroup_factdetailmain  d;
)
SELECT {{ dbt_utils.generate_surrogate_key(['d._recid', 'd._sourceid']) }} AS Tagcostgroupkey
   , d.legalentitykey
   , d.tagkey
   , d.productkey
   , d.costgroupkey
   , d.cmacostingunitkey
   , d.costgroupid
   , d.inventqty
   , d.costingqty
   , d.costamount
   , d.unitcost
   , d._sourceid
   , d._recid
   , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
   , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
FROM tagcostgroup_factdetail1  d;
