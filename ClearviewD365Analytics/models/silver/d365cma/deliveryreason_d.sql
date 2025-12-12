{{ config(materialized='table', tags=['silver'], alias='deliveryreason') }}

-- Source file: cma/cma/layers/_base/_silver/deliveryreason/deliveryreason.py
-- Root method: Deliveryreason.deliveryreasondetail [DeliveryReasonDetail]
-- external_table_name: DeliveryReasonDetail
-- schema_name: temp

SELECT 
        {{ dbt_utils.generate_surrogate_key(['dm.recid']) }} AS DeliveryReasonKey
         ,dm.dataareaid AS LegalEntityID
         , dm.code        AS DeliveryReasonID
         , dm.txt         AS DeliveryReason
         , dm.recid      AS _RecID
         , 1              AS _SourceID         
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
     FROM {{ ref('dlvreason') }} dm
     WHERE dm.code <> '';

