{{ config(materialized='view') }}

SELECT DISTINCT
      optionsetname AS enum
    , enumvalue     AS enumvalue
    , enumvalueid   AS enumid
    , externalvalue AS enumvalueid
FROM {{ source("lakehouse","globalenummetadata") }}
