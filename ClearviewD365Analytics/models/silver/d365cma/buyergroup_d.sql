{{ config(materialized='table', tags=['silver'], alias='buyergroup') }}

-- Source file: cma/cma/layers/_base/_silver/buyergroup/buyergroup.py
-- Root method: Buyergroup.buyergroupdetail [BuyerGroupDetail]
-- external_table_name: BuyerGroupDetail
-- schema_name: temp

SELECT ROW_NUMBER() OVER (ORDER BY ib.recid) AS BuyerGroupKey
        ,ib.dataareaid                                                      AS LegalEntityID
         , ib.[GROUP]                                                           AS BuyerGroupID
         , CASE WHEN ib.description = '' THEN ib.[GROUP] ELSE ib.description END AS BuyerGroup
         , ib.recid                                                             AS _RecID
         , 1                                                                    AS _SourceID
        ,CURRENT_TIMESTAMP                                               AS _CreatedDate
        , CURRENT_TIMESTAMP                                               AS _ModifiedDate

      FROM {{ ref('inventbuyergroup') }} ib
     WHERE ib.description <> '';

