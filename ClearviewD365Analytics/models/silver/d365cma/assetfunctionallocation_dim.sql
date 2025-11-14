{{ config(materialized='table', tags=['silver']) }}

-- Source file: cma/cma/layers/_base/_silver/assetfunctionallocation/assetfunctionallocation.py
-- Root method: Assetfunctionallocation.assetfunctionallocationdetail [AssetFunctionalLocationDetail]
-- external_table_name: AssetFunctionalLocationDetail
-- schema_name: temp

SELECT ROW_NUMBER() OVER (ORDER BY FL.recid) AS AssetFunctionalLocationKey
    , FL.dataareaid                                        AS LegalEntityID
         , FL.functionallocationid                                AS FunctionalLocationID
         , ISNULL(NULLIF(FL.name, ''), FL.functionallocationid)   AS FunctionalLocation
         , flt.name                                               AS LevelType
         , flh.level                                             AS Level
         , fl1.functionallocationid                               AS Level1ID
         , ISNULL(NULLIF(fl1.name, ''), fl1.functionallocationid) AS Level1
         , flt1.name                                              AS Level1Type
         , fl2.functionallocationid                               AS Level2ID
         , ISNULL(NULLIF(fl2.name, ''), fl2.functionallocationid) AS Level2
         , flt2.name                                              AS Level2Type
         , fl3.functionallocationid                               AS Level3ID
         , ISNULL(NULLIF(fl3.name, ''), fl3.functionallocationid) AS Level3
         , flt3.name                                              AS Level3Type
         , fl4.functionallocationid                               AS Level4ID
         , ISNULL(NULLIF(fl4.name, ''), fl4.functionallocationid) AS Level4
         , flt4.name                                              AS Level4Type
         , fl5.functionallocationid                               AS Level5ID
         , ISNULL(NULLIF(fl5.name, ''), fl5.functionallocationid) AS Level5
         , flt5.name                                              AS Level5Type
         , fl6.functionallocationid                               AS Level6ID
         , ISNULL(NULLIF(fl6.name, ''), fl6.functionallocationid) AS Level6
         , flt6.name                                              AS Level6Type
         , fl7.functionallocationid                               AS Level7ID
         , ISNULL(NULLIF(fl7.name, ''), fl7.functionallocationid) AS Level7
         , flt7.name                                              AS Level7Type
         , fl8.functionallocationid                               AS Level8ID
         , ISNULL(NULLIF(fl8.name, ''), fl8.functionallocationid) AS Level8
         , flt8.name                                              AS Level8Type
         , fl9.functionallocationid                               AS Level9ID
         , ISNULL(NULLIF(fl9.name, ''), fl9.functionallocationid) AS Level9
         , flt9.name                                              AS Level9Type
         , FL.recid                                              AS _RecID
         , 1                                                      AS _SourceID
         ,  CURRENT_TIMESTAMP                                       AS  _CreatedDate
         ,  CURRENT_TIMESTAMP                                        AS  _ModifiedDate

      FROM {{ ref('entassetfunctionallocation') }}               FL
      LEFT JOIN {{ ref('entassetfunctionallocationtype') }}      flt
        ON flt.recid               = FL.functionallocationtype
      LEFT JOIN {{ ref('entassetfunctionallocationhierarchy') }} flh
        ON flh.functionallocation    = FL.recid
       AND flh.functionallocationref = FL.recid
      LEFT JOIN {{ ref('entassetfunctionallocationhierarchy') }} l1
        ON l1.dataareaid           = FL.dataareaid
       AND l1.functionallocationref  = flh.functionallocationref
       AND l1.level                = 1
      LEFT JOIN {{ ref('entassetfunctionallocation') }}          fl1
        ON fl1.recid               = l1.functionallocation
      LEFT JOIN {{ ref('entassetfunctionallocationtype') }}      flt1
        ON flt1.recid             = fl1.functionallocationtype
      LEFT JOIN {{ ref('entassetfunctionallocationhierarchy') }} l2
        ON l2.dataareaid            = FL.dataareaid
       AND l2.functionallocationref  = flh.functionallocationref
       AND l2.level                 = 2
      LEFT JOIN {{ ref('entassetfunctionallocation') }}          fl2
        ON fl2.recid                = l2.functionallocation
      LEFT JOIN {{ ref('entassetfunctionallocationtype') }}      flt2
        ON flt2.recid              = fl2.functionallocationtype
      LEFT JOIN {{ ref('entassetfunctionallocationhierarchy') }} l3
        ON l3.dataareaid           = FL.dataareaid
       AND l3.functionallocationref  = flh.functionallocationref
       AND l3.level                = 3
      LEFT JOIN {{ ref('entassetfunctionallocation') }}          fl3
        ON fl3.recid                = l3.functionallocation
      LEFT JOIN {{ ref('entassetfunctionallocationtype') }}      flt3
        ON flt3.recid              = fl3.functionallocationtype
      LEFT JOIN {{ ref('entassetfunctionallocationhierarchy') }} l4
        ON l4.dataareaid           = FL.dataareaid
       AND l4.functionallocationref  = flh.functionallocationref
       AND l4.level                 = 4
      LEFT JOIN {{ ref('entassetfunctionallocation') }}          fl4
        ON fl4.recid              = l4.functionallocation
      LEFT JOIN {{ ref('entassetfunctionallocationtype') }}      flt4
        ON flt4.recid               = fl4.functionallocationtype
      LEFT JOIN {{ ref('entassetfunctionallocationhierarchy') }} l5
        ON l5.dataareaid           = FL.dataareaid
       AND l5.functionallocationref  = flh.functionallocationref
       AND l5.level                 = 5
      LEFT JOIN {{ ref('entassetfunctionallocation') }}          fl5
        ON fl5.recid               = l5.functionallocation
      LEFT JOIN {{ ref('entassetfunctionallocationtype') }}      flt5
        ON flt5.recid              = fl5.functionallocationtype
      LEFT JOIN {{ ref('entassetfunctionallocationhierarchy') }} l6
        ON l6.dataareaid           = FL.dataareaid
       AND l6.functionallocationref  = flh.functionallocationref
       AND l6.level                = 6
      LEFT JOIN {{ ref('entassetfunctionallocation') }}          fl6
        ON fl6.recid               = l6.functionallocation
      LEFT JOIN {{ ref('entassetfunctionallocationtype') }}      flt6
        ON flt6.recid               = fl6.functionallocationtype
      LEFT JOIN {{ ref('entassetfunctionallocationhierarchy') }} l7
        ON l7.dataareaid          = FL.dataareaid
       AND l7.functionallocationref  = flh.functionallocationref
       AND l7.level                 = 7
      LEFT JOIN {{ ref('entassetfunctionallocation') }}          fl7
        ON fl7.recid                = l7.functionallocation
      LEFT JOIN {{ ref('entassetfunctionallocationtype') }}      flt7
        ON flt7.recid               = fl7.functionallocationtype
      LEFT JOIN {{ ref('entassetfunctionallocationhierarchy') }} l8    
        ON l8.dataareaid           = FL.dataareaid
       AND l8.functionallocationref  = flh.functionallocationref
       AND l8.level                 = 8
      LEFT JOIN {{ ref('entassetfunctionallocation') }}          fl8
        ON fl8.recid              = l8.functionallocation
      LEFT JOIN {{ ref('entassetfunctionallocationtype') }}      flt8
        ON flt8.recid               = fl8.functionallocationtype
      LEFT JOIN {{ ref('entassetfunctionallocationhierarchy') }} l9
        ON l9.dataareaid            = FL.dataareaid
       AND l9.functionallocationref  = flh.functionallocationref
       AND l9.level                 = 9
      LEFT JOIN {{ ref('entassetfunctionallocation') }}          fl9
        ON fl9.recid               = l9.functionallocation
      LEFT JOIN {{ ref('entassetfunctionallocationtype') }}      flt9
        ON flt9.recid               = fl9.functionallocationtype;
