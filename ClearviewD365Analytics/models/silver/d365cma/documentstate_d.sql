{{ config(materialized='table', tags=['silver'], alias='documentstate') }}

WITH detail AS (
    SELECT we.EnumValueID AS DocumentStateID
         , we.EnumValue   AS DocumentState
      FROM {{ ref('enumeration') }} we
     WHERE we.Enum = 'VersioningDocumentState'
)

SELECT DocumentStateID
     , DocumentState
  FROM detail
 ORDER BY DocumentStateID;
