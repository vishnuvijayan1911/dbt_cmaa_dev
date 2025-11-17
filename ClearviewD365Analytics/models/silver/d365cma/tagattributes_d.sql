{{ config(materialized='table', tags=['silver'], alias='tagattribute') }}

-- Source file: cma/cma/layers/_base/_silver/tagattributes/tagattributes.py
-- Root method: Tagattribute.tagattributedetail [TagAttributeDetail]
-- Inlined methods: Tagattribute.tagattributedetail1 [TagAttributesDetail1]
-- external_table_name: TagAttributeDetail
-- schema_name: temp

WITH
tagattributesdetail1 AS (
    SELECT ba.dataareaid                                                                                  AS LegalEntityID
             , ba.inventbatchid                                                                                AS TagID
             , ba.itemid                                                                                       AS ItemID
             , {{ get_tag_attr_def_for_detail1() }}
          FROM {{ ref('inventbatch') }}             ib
         INNER JOIN {{ ref('pdsbatchattributes') }} ba
            ON ba.dataareaid   = ib.dataareaid
           AND ba.inventbatchid = ib.inventbatchid
           AND ba.itemid        = ib.itemid
         WHERE LTRIM(RTRIM(ba.pdsbatchattribvalue)) <> ''
           AND ib.inventbatchid                     <> ''
         GROUP BY ba.dataareaid
                , ba.inventbatchid
                , ba.itemid
)
SELECT ib.*
        ,{{ get_tag_attr_def_for_detail() }}
    FROM silver.cma_Tag                     ib

    LEFT JOIN tagattributesdetail1                           tt
        ON tt.LegalEntityID    = ib.legalentityid
    AND tt.TagID            = ib.tagid
    AND tt.ItemID           = ib.itemid

    WHERE ib.tagid <> '0'
    AND ib.tagid <> '';

