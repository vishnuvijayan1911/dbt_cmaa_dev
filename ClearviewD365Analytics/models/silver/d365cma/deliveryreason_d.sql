{{ config(materialized='table', tags=['silver'], alias='deliveryreason_dim') }}

-- Source file: cma/cma/layers/_base/_silver/deliveryreason/deliveryreason.py
-- Root method: Deliveryreason.deliveryreasondetail [DeliveryReasonDetail]
-- external_table_name: DeliveryReasonDetail
-- schema_name: temp

SELECT 
        ROW_NUMBER() OVER (ORDER BY dm.recid) AS DeliveryReasonKey
         ,dm.dataareaid AS LegalEntityID
         , dm.code        AS DeliveryReasonID
         , dm.txt         AS DeliveryReason
         , dm.recid      AS _RecID
         , 1              AS _SourceID         
        ,CURRENT_TIMESTAMP                                               AS _CreatedDate
        , CURRENT_TIMESTAMP                                               AS _ModifiedDate
     FROM {{ ref('dlvreason') }} dm
     WHERE dm.code <> '';
