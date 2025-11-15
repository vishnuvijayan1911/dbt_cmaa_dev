{{ config(materialized='table', tags=['silver'], alias='deliveryterm_dim') }}

-- Source file: cma/cma/layers/_base/_silver/deliveryterm/deliveryterm.py
-- Root method: Deliveryterm.deliverytermdetail [DeliveryTermDetail]
-- external_table_name: DeliveryTermDetail
-- schema_name: temp

SELECT ROW_NUMBER() OVER (ORDER BY tm.recid) AS DeliveryTermKey
        , tm.dataareaid                                      AS LegalEntityID
         , tm.code                                            AS DeliveryTermID
         , CASE WHEN tm.txt = '' THEN tm.code ELSE tm.txt END AS DeliveryTerm
         , tm.recid                                           AS _RecID
         , 1                                                  AS _SourceID
        ,CURRENT_TIMESTAMP                                               AS _CreatedDate
        , CURRENT_TIMESTAMP                                               AS _ModifiedDate

      FROM {{ ref('dlvterm') }} tm
     WHERE tm.code <> '';
