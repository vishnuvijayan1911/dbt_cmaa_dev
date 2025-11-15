{{ config(materialized='table', tags=['silver'], alias='on_time_status_dim') }}

-- Source file: cma/cma/layers/_base/_silver/dimension_tables/dimension_tables.py
-- Root method: DimensionTables.on_time_status

SELECT t.OnTimeStatusID
 ,CAST(t.OnTimeStatus AS varchar(100)) AS OnTimeStatus
 ,t.OnTime
 ,CAST(t.ProcessStatus AS varchar(50))  AS ProcessStatus
 ,CAST(t.OnTimeStatusType AS varchar(100))  AS OnTimeStatusType
     FROM (
               ---------------Work Order Status (Due date)--------------------
               SELECT 1                              AS OnTimeStatusID
                    , 'Not yet due'                  AS OnTimeStatus
                    , 'Not due'                      AS OnTime
                    , 'Not complete'                 AS ProcessStatus
                    , 'Work Order Status (Due date)' AS OnTimeStatusType
               UNION ALL
               SELECT 2                              AS OnTimeStatusID
                    , 'Past due'                     AS OnTimeStatus
                    , 'Late'                         AS OnTime
                    , 'Not complete'                 AS ProcessStatus
                    , 'Work Order Status (Due date)' AS OnTimeStatusType
               UNION ALL
               SELECT 3                              AS OnTimeStatusID
                    , 'Completed late'               AS OnTimeStatus
                    , 'Late'                         AS OnTime
                    , 'Complete'                     AS ProcessStatus
                    , 'Work Order Status (Due date)' AS OnTimeStatusType
               UNION ALL
               SELECT 4                              AS OnTimeStatusID
                    , 'Completed on-time'            AS OnTimeStatus
                    , 'On-time'                      AS OnTime
                    , 'Complete'                     AS ProcessStatus
                    , 'Work Order Status (Due date)' AS OnTimeStatusType
               UNION ALL
               SELECT 5                              AS OnTimeStatusID
                    , 'Completed (no due date)'      AS OnTimeStatus
                    , 'On-time'                      AS OnTime
                    , 'Complete'                     AS ProcessStatus
                    , 'Work Order Status (Due date)' AS OnTimeStatusType
               UNION ALL
               SELECT 6                              AS OnTimeStatusID
                    , 'Open (no due date)'           AS OnTimeStatus
                    , 'Not due'                      AS OnTime
                    , 'Not complete'                 AS ProcessStatus
                    , 'Work Order Status (Due date)' AS OnTimeStatusType

               ---------------Work Order Status (Start date)--------------------
               UNION ALL
               SELECT 7                                AS OnTimeStatusID
                    , 'Not yet due to start'           AS OnTimeStatus
                    , 'Not due to start'               AS OnTime
                    , 'Not started'                    AS ProcessStatus
                    , 'Work Order Status (Start date)' AS OnTimeStatusType
               UNION ALL
               SELECT 8                                AS OnTimeStatusID
                    , 'Past due to start'              AS OnTimeStatus
                    , 'Late start'                     AS OnTime
                    , 'Not complete'                   AS ProcessStatus
                    , 'Work Order Status (Start date)' AS OnTimeStatusType
               UNION ALL
               SELECT 9                                AS OnTimeStatusID
                    , 'Started late'                   AS OnTimeStatus
                    , 'Late start'                     AS OnTime
                    , 'Started'                        AS ProcessStatus
                    , 'Work Order Status (Start date)' AS OnTimeStatusType
               UNION ALL
               SELECT 10                               AS OnTimeStatusID
                    , 'Started on-time'                AS OnTimeStatus
                    , 'On-time start'                  AS OnTime
                    , 'Started'                        AS ProcessStatus
                    , 'Work Order Status (Start date)' AS OnTimeStatusType
               UNION ALL
               SELECT 11                                              AS OnTimeStatusID
                    , 'Started (no expected or scheduled start date)' AS OnTimeStatus
                    , 'On-time start'                                 AS OnTime
                    , 'Started'                                       AS ProcessStatus
                    , 'Work Order Status (Start date)'                AS OnTimeStatusType
               UNION ALL
               SELECT 12                                                  AS OnTimeStatusID
                    , 'Not started (no expected or scheduled start date)' AS OnTimeStatus
                    , 'Not due to start'                                  AS OnTime
                    , 'Not started'                                       AS ProcessStatus
                    , 'Work Order Status (Start date)'                    AS OnTimeStatusType) t;
