{{ config(materialized='table', tags=['silver'], alias='approvalstatus') }}

WITH detail AS (
    SELECT CAST(0 AS INT) AS ApprovalStatusID
         , 'Not approved'  AS ApprovalStatus
    UNION ALL
    SELECT CAST(1 AS INT) AS ApprovalStatusID
         , 'Approved'      AS ApprovalStatus
)

SELECT ApprovalStatusID
     , ApprovalStatus
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
  FROM detail
