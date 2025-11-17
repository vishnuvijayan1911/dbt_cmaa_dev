---{{ config(materialized='table', tags=['silver'], alias='deliverymode') }}

-- Source file: cma/cma/layers/_base/_silver/deliverymode/deliverymode.py
-- Root method: Deliverymode.deliverymodedetail [DeliveryModeDetail]
-- external_table_name: DeliveryModeDetail
-- schema_name: temp

SELECT {{ dbt_utils.generate_surrogate_key(["dm.recid"]) }}            AS DeliveryModeKey
        , dm.dataareaid                                      AS LegalEntityID
         , dm.code                                            AS DeliveryModeID
         , CASE WHEN dm.txt = '' THEN dm.code ELSE dm.txt END AS DeliveryMode
         , dm.recid                                           AS _RecID
         , 1                                                  AS _SourceID
         ,  cast(CURRENT_TIMESTAMP  as DATETIME2(6))                                              AS _CreatedDate
         , cast(CURRENT_TIMESTAMP  as DATETIME2(6))                                              AS _ModifiedDate
      FROM {{ ref('dlvmode') }} dm
     WHERE dm.code <> '';

