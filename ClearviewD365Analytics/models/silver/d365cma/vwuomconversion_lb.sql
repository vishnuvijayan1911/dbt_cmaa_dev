{{ config(materialized='table', tags=['silver'], alias='vwuomconversion_lb') }}

SELECT *
    , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
    , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
  FROM {{ ref('uom_d') }};

