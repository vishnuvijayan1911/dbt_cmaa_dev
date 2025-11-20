{{ config(materialized='table', tags=['silver'], alias='inventory_trans_status') }}

-- Source file: cma/cma/layers/_base/_silver/dimension_tables/dimension_tables.py
-- Root method: DimensionTables.inventory_trans_status

SELECT 
, CASE WHEN we.enum = 'StatusIssue' THEN 1 ELSE 2 END                                                             AS InventoryTransStatusTypeID
, CASE WHEN we.enum = 'StatusIssue' THEN 'Issue' ELSE 'Receipt' END                                               AS InventoryTransStatusType
, we.enumvalueid                                                                                                  AS InventoryTransStatusID
, CASE WHEN we.enum = 'StatusIssue' THEN 'Issue' + ' - ' + we.enumvalue ELSE 'Receipt' + ' - ' + we.enumvalue END AS InventoryTransStatus
, we.enumvalue                                                                                                    AS InventoryTransStatusName
, ISNULL(CASE WHEN we.enum = 'StatusIssue'
               AND we.enumvalueid IN ( 7 )
               THEN 'Quoted'
               WHEN we.enum = 'StatusIssue'
               AND we.enumvalueid IN ( 6 )
               THEN 'On order'
               WHEN we.enum = 'StatusIssue'
               AND we.enumvalueid IN ( 4 )
               THEN 'Reserved physical'
               WHEN we.enum = 'StatusIssue'
               AND we.enumvalueid IN ( 5 )
               THEN 'Reserved ordered'
               WHEN we.enum = 'StatusIssue'
               AND we.enumvalueid IN ( 3 )
               THEN 'Picked not yet shipped'
               WHEN we.enum = 'StatusIssue'
               AND we.enumvalueid IN ( 2 )
               THEN 'Shipped not yet invoiced'
               WHEN we.enum = 'StatusIssue'
               AND we.enumvalueid IN ( 1 )
               THEN 'Sold'
               WHEN we.enum = 'StatusReceipt'
               AND we.enumvalueid IN ( 6 )
               THEN 'Quoted'
               WHEN we.enum = 'StatusReceipt'
               AND we.enumvalueid IN ( 5 )
               THEN 'Ordered'
               WHEN we.enum = 'StatusReceipt'
               AND we.enumvalueid IN ( 4 )
               THEN 'Arrived'
               WHEN we.enum = 'StatusReceipt'
               AND we.enumvalueid IN ( 3 )
               THEN 'Registered'
               WHEN we.enum = 'StatusReceipt'
               AND we.enumvalueid IN ( 2 )
               THEN 'Received not yet invoiced'
               WHEN we.enum = 'StatusReceipt'
               AND we.enumvalueid IN ( 1 )
               THEN 'Purchased' END
          , '')                                                                                                      AS InventoryTransStatusDesc
, ISNULL(CASE WHEN we.enum = 'StatusIssue'
               AND we.enumvalueid IN ( 3, 4, 5, 6 )
               THEN 1
               WHEN we.enum = 'StatusIssue'
               AND we.enumvalueid IN ( 1, 2 )
               THEN 2
               WHEN we.enum = 'StatusReceipt'
               AND we.enumvalueid IN ( 3, 4, 5 )
               THEN 1
               WHEN we.enum = 'StatusReceipt'
               AND we.enumvalueid IN ( 1, 2 )
               THEN 3 END
          , '')                                                                                                      AS InventoryTransStatusGroupID
, ISNULL(CASE WHEN we.enum = 'StatusIssue'
               AND we.enumvalueid IN ( 3, 4, 5, 6 )
               THEN 'Open'
               WHEN we.enum = 'StatusIssue'
               AND we.enumvalueid IN ( 1, 2 )
               THEN 'Shipped'
               WHEN we.enum = 'StatusReceipt'
               AND we.enumvalueid IN ( 3, 4, 5 )
               THEN 'Open'
               WHEN we.enum = 'StatusReceipt'
               AND we.enumvalueid IN ( 1, 2 )
               THEN 'Received' END
          , '')                                                                                                      AS InventoryTransStatusGroup
     cast(CURRENT_TIMESTAMP as DATETIME2(6))                         AS _CreatedDate
, cast(CURRENT_TIMESTAMP as DATETIME2(6))                          AS _ModifiedDate
FROM {{ ref('enumeration') }} we
WHERE we.enum IN ( 'StatusIssue', 'StatusReceipt' )
AND we.enumvalueid <> 0

