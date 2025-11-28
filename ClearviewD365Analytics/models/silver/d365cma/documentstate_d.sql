{{ config(materialized='table', tags=['silver'], alias='documentstate') }}

WITH detail AS (
    SELECT we.enumid     AS DocumentStateID  -- Changed this mapping due to enum workaround for fabric.
         , we.enumvalue   AS DocumentState
      FROM {{ ref('enumeration') }} we
     WHERE we.enum = 'VersioningDocumentState'
)
SELECT DocumentStateID
     , DocumentState
  FROM detail;