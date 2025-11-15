{{ config(materialized='table', tags=['silver'], alias='approval_status_dim') }}

-- Source file: cma/cma/layers/_base/_silver/dimension_tables/dimension_tables.py
-- Root method: DimensionTables.approval_status

SELECT
          0 AS ApprovalStatusID ,
          CAST('Not approved' AS VARCHAR(60)) AS ApprovalStatus, 
          CURRENT_TIMESTAMP  AS _CreatedDate,
          CURRENT_TIMESTAMP  AS _ModifiedDate
UNION
     SELECT
               1 AS ApprovalStatusID ,
               CAST('Approved' AS VARCHAR(60)) ApprovalStatus, 
               CURRENT_TIMESTAMP AS _CreatedDate,
               CURRENT_TIMESTAMP AS _ModifiedDate
