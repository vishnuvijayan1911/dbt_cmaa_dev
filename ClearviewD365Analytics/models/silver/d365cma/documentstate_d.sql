{{ config(materialized='table', tags=['silver'], alias='documentstate') }}

WITH detail AS (
    SELECT we.enumid     AS DocumentStateID  -- Changed this mapping due to enum workaround for fabric.
         , we.enumvalue   AS DocumentState
      FROM {{ ref('enumeration') }} we
     WHERE we.enum = 'documentstate'
)
SELECT DocumentStateID
     , DocumentState
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
  FROM detail
