{{ config(materialized='table', tags=['silver'], alias='deliveryterm') }}

-- Source file: cma/cma/layers/_base/_silver/deliveryterm/deliveryterm.py
-- Root method: Deliveryterm.deliverytermdetail [DeliveryTermDetail]
-- external_table_name: DeliveryTermDetail
-- schema_name: temp

SELECT {{ dbt_utils.generate_surrogate_key(['tm.recid']) }} AS DeliveryTermKey
        , tm.dataareaid                                      AS LegalEntityID
         , tm.code                                            AS DeliveryTermID
         , CASE WHEN tm.txt = '' THEN tm.code ELSE tm.txt END AS DeliveryTerm
         , tm.recid                                           AS _RecID
         , 1                                                  AS _SourceID

        ,cast(CURRENT_TIMESTAMP as DATETIME2(6))                                               AS _CreatedDate
        , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                               AS _ModifiedDate
      FROM {{ ref('dlvterm') }} tm
     WHERE tm.code <> '';

