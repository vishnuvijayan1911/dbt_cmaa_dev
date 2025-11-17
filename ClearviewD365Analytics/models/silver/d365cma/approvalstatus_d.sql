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
  FROM detail
 ORDER BY ApprovalStatusID;
