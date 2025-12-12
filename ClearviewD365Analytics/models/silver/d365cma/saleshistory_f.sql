{{ config(materialized='table', tags=['silver'], alias='saleshistory_fact') }}

-- Source file: cma/cma/layers/_base/_silver/saleshistory_f/saleshistory_f.py
-- Root method: SaleshistoryFact.saleshistory_factdetail [SalesHistory_FactDetail]
-- Inlined methods: SaleshistoryFact.saleshistory_facthistory [SalesHistory_FactHistory], SaleshistoryFact.saleshistory_factstage [SalesHistory_FactStage]
-- external_table_name: SalesHistory_FactDetail
-- schema_name: temp

WITH
saleshistory_facthistory AS (
    SELECT t.SalesOrderLineKey
             , t.LegalEntityKey
             , t.SalesStatusKey
             , t.OrigBaseAmount
             , t.OrigBaseAmount_TransCur
             , t.OrigOrderedQuantity
             , t.OrigOrderedQuantity_SalesUOM
             , t.OrigOrderedQuantity_LB

             , t.OrigOrderedSalesAmount
             , t.OrigOrderedSalesAmount_TransCur
             , t.BookDate
             , t.OrigShipDateRequested
             , t._RecID

          FROM (   SELECT sh.SalesOrderLineKey              AS SalesOrderLineKey
                        , sh.LegalEntityKey                 AS LegalEntityKey
                        , sh.SalesStatusKey                 AS SalesStatusKey
                        , sh.NewBaseAmount                  AS OrigBaseAmount
                        , sh.NewBaseAmount_TransCur         AS OrigBaseAmount_TransCur
                        , sh.NewOrderedQuantity             AS OrigOrderedQuantity
                        , sh.NewOrderedQuantity_SalesUOM    AS OrigOrderedQuantity_SalesUOM
                        , sh.NewOrderedQuantity_LB          AS OrigOrderedQuantity_LB

                        , sh.NewOrderedSalesAmount          AS OrigOrderedSalesAmount
                        , sh.NewOrderedSalesAmount_TransCur AS OrigOrderedSalesAmount_TransCur
                        , sh.BookDate                       AS BookDate
                        , sh.NewShipDateRequested           AS OrigShipDateRequested
                        , ROW_NUMBER () OVER (PARTITION BY sh._RecID
    ORDER BY sh.BookDate DESC)                              AS RankVal
                        , sh._RecID                         AS _RecID
                     FROM {{ this }} sh


          ) AS t
         WHERE t.RankVal = 1;
),
saleshistory_factstage AS (
    SELECT t.SalesOrderLineKey
             , t.LegalEntityKey
             , t.SalesStatusKey
             , t.NewBaseAmount
             , t.NewBaseAmount_TransCur
             , t.NewOrderedQuantity
             , t.NewOrderedQuantity_SalesUOM
             , t.NewOrderedQuantity_LB

             , t.NewOrderedSalesAmount
             , t.NewOrderedSalesAmount_TransCur
             , t.NewShipDateRequested
             , SalesUpdateTypeID
             , t._RecID

          FROM (   SELECT sol.SalesOrderLineKey           AS SalesOrderLineKey
                        , sol.LegalEntityKey              AS LegalEntityKey
                        , sol.SalesLineStatusKey          AS SalesStatusKey
                        , sol.BaseAmount                  AS NewBaseAmount
                        , sol.BaseAmount_TransCur         AS NewBaseAmount_TransCur
                        , sol.OrderedQuantity             AS NewOrderedQuantity
                        , sol.OrderedQuantity_SalesUOM    AS NewOrderedQuantity_SalesUOM
                        , sol.OrderedQuantity_LB          AS NewOrderedQuantity_LB

                        , sol.OrderedSalesAmount          AS NewOrderedSalesAmount
                        , sol.OrderedSalesAmount_TransCur AS NewOrderedSalesAmount_TransCur
                        , CAST(dd.Date AS DATE)           AS NewShipDateRequested
                        , CASE WHEN (sol.OrderedQuantity <> th.OrigOrderedQuantity)
                               THEN 2
                               WHEN CAST(dd.Date AS DATE) <> OrigShipDateRequested
                               THEN 3
                               WHEN (sol.OrderedQuantity <> th.OrigOrderedQuantity)
                                AND CAST(dd.Date AS DATE) <> OrigShipDateRequested
                               THEN 4
                               ELSE 1 END                 AS SalesUpdateTypeID
                        , sol._RecID                      AS _RecID
                     FROM {{ ref('salesorderline_f') }} sol

                    INNER JOIN {{ ref('date_d') }}           dd
                       ON dd.DateKey = sol.ShipDateRequestedKey
                     LEFT JOIN saleshistory_facthistory           th
                       ON th._RecID  = sol._RecID
                    WHERE sol.SalesLineStatusKey <> 5
                   UNION ALL
                   (SELECT t1.SalesOrderLineKey              AS SalesOrderLineKey
                         , t1.LegalEntityKey                 AS LegalEntityKey
                         , t1.SalesStatusKey                 AS SalesStatusKey
                         , t1.NewBaseAmount                  AS NewBaseAmount
                         , t1.NewBaseAmount_TransCur         AS NewBaseAmount_TransCur
                         , t1.NewOrderedQuantity             AS NewOrderedQuantity
                         , t1.NewOrderedQuantity_SalesUOM    AS NewOrderedQuantity_SalesUOM
                         , t1.NewOrderedQuantity_LB          AS NewOrderedQuantity_LB

                         , t1.NewOrderedSalesAmount          AS NewOrderedSalesAmount
                         , t1.NewOrderedSalesAmount_TransCur AS NewOrderedSalesAmount_TransCur
                         , t1.NewShipDateRequested           AS NewShipDateRequested
                         , t1.SalesUpdateTypeID              AS SalesUpdateTypeID
                         , t1._RecID                         AS _RecID
                      FROM (   SELECT sh.SalesOrderLineKey       AS SalesOrderLineKey
                                    , sh.LegalEntityKey          AS LegalEntityKey
                                    , -1                         AS SalesStatusKey
                                    , 0                          AS NewBaseAmount
                                    , 0                          AS NewBaseAmount_TransCur
                                    , 0                          AS NewOrderedQuantity
                                    , 0                          AS NewOrderedQuantity_SalesUOM
                                    , 0                          AS NewOrderedQuantity_LB

                                    , 0                          AS NewOrderedSalesAmount
                                    , 0                          AS NewOrderedSalesAmount_TransCur
                                    , CAST('01/01/1900' AS DATE) AS NewShipDateRequested
                                    , 6                          AS SalesUpdateTypeID
                                    , ROW_NUMBER () OVER (PARTITION BY sh._RecID
    ORDER BY sh.BookDate DESC       )                            AS RankVal
                                    , sh._RecID                  AS _RecID
                                 FROM (   SELECT *
                                            FROM {{ this }}
                                           WHERE _RecID NOT IN ( SELECT DISTINCT _RecID FROM {{ this }} WHERE ModifiedBy = '' )) sh
                                 LEFT JOIN {{ ref('salesorderline_f') }}                                                                          sol
                                   ON sol._RecID = sh._RecID
                                 LEFT JOIN {{ ref('salesline') }}                                                                                    sl
                                   ON sl.recid  = sh._RecID
                                WHERE sol._RecID IS NULL
                                  AND sl.salesstatus <> 4) AS t1
                     WHERE t1.RankVal = 1)
                   UNION ALL
                   SELECT th.SalesOrderLineKey       AS SalesOrderLineKey
                        , th.LegalEntityKey          AS LegalEntityKey
                        , ss.SalesStatusKey          AS SalesStatusKey
                        , 0                          AS NewBaseAmount
                        , 0                          AS NewBaseAmount_TransCur
                        , 0                          AS NewOrderedQuantity
                        , 0                          AS NewOrderedQuantity_SalesUOM
                        , 0                          AS NewOrderedQuantity_LB

                        , 0                          AS NewOrderedSalesAmount
                        , 0                          AS NewOrderedSalesAmount_TransCur
                        , CAST('01/01/1900' AS DATE) AS NewShipDateRequested
                        , 5                          AS SalesUpdateTypeID
                        , th._RecID                  AS _RecID
                     FROM saleshistory_facthistory             th
                     LEFT JOIN {{ ref('salesline') }}   sl
                       ON sl.recid        = th._RecID
                     LEFT JOIN {{ ref('salesstatus_d') }} ss
                       ON ss.SalesStatusID = sl.salesstatus
                    WHERE sl.salesstatus    = 4
                      AND th.SalesStatusKey <> 5) AS t;
)
SELECT ROW_NUMBER () OVER (ORDER BY t._RecID ) AS SalesHistoryKey, * FROM (
    SELECT 
          ,ISNULL (ts.SalesOrderLineKey, th.SalesOrderLineKey)                                                         AS SalesOrderLineKey
         , ISNULL (ts.SalesStatusKey, th.SalesStatusKey)                                                               AS SalesStatusKey
         , st.SalesUpdateTypeKey                                                                                       AS SalesUpdateTypeKey
         , ISNULL (ts.LegalEntityKey, th.LegalEntityKey)                                                               AS LegalEntityKey
         , ts.NewBaseAmount - ISNULL (th.OrigBaseAmount, 0)                                                            AS DeltaBaseAmount
         , CAST(th.OrigBaseAmount AS DECIMAL(19,4))                                                                                          AS OrigBaseAmount
         , ts.NewBaseAmount                                                                                            AS NewBaseAmount
         , ts.NewBaseAmount_TransCur - ISNULL (th.OrigBaseAmount_TransCur, 0)                                          AS DeltaBaseAmount_TransCur
         , CAST(th.OrigBaseAmount_TransCur AS DECIMAL(19,4))                                                                                  AS OrigBaseAmount_TransCur
         , ts.NewBaseAmount_TransCur                                                                                   AS NewBaseAmount_TransCur
         , ts.NewOrderedQuantity - ISNULL (th.OrigOrderedQuantity, 0)                                                  AS DeltaOrderedQuantity
         , CAST(th.OrigOrderedQuantity AS NUMERIC(20,6))                                                                                     AS OrigOrderedQuantity
         , ts.NewOrderedQuantity                                                                                       AS NewOrderedQuantity
         , ts.NewOrderedQuantity_SalesUOM - ISNULL (th.OrigOrderedQuantity_SalesUOM, 0)                                AS DeltaOrderedQuantity_SalesUOM
         , CAST(th.OrigOrderedQuantity_SalesUOM AS NUMERIC(20,6))                                                                            AS OrigOrderedQuantity_SalesUOM
         , ts.NewOrderedQuantity_SalesUOM                                                                              AS NewOrderedQuantity_SalesUOM
         , ts.NewOrderedQuantity_LB - ISNULL (th.OrigOrderedQuantity_LB, 0)                                            AS DeltaOrderedQuantity_LB
         , CAST(th.OrigOrderedQuantity_LB  AS NUMERIC(20,6))                                                                                 AS OrigOrderedQuantity_LB
         , ts.NewOrderedQuantity_LB                                                                                    AS NewOrderedQuantity_LB



         , ts.NewOrderedSalesAmount - ISNULL (th.OrigOrderedSalesAmount, 0)                                            AS DeltaOrderedSalesAmount
         , CAST(th.OrigOrderedSalesAmount AS DECIMAL(19,4))                                                                                  AS OrigOrderedSalesAmount
         , ts.NewOrderedSalesAmount                                                                                    AS NewOrderedSalesAmount
         , ts.NewOrderedSalesAmount_TransCur - ISNULL (th.OrigOrderedSalesAmount_TransCur, 0)                          AS DeltaOrderedSalesAmount_TransCur
         , CAST(th.OrigOrderedSalesAmount_TransCur AS DECIMAL(19,4))                                                                         AS OrigOrderedSalesAmount_TransCur
         , ts.NewOrderedSalesAmount_TransCur                                                                           AS NewOrderedSalesAmount_TransCur
         , CASE WHEN ts.NewShipDateRequested = CAST('01/01/1900' AS DATE)
                THEN NULL
                ELSE DATEDIFF (DAY, CAST(th.OrigShipDateRequested AS DATE), CAST(ts.NewShipDateRequested AS DATE)) END AS DeltaShipDaysRequested
         , CAST(CASE WHEN th.OrigShipDateRequested  = '' THEN '1900-01-01 00:00:00'
			ELSE th.OrigShipDateRequested END AS DATE	)																	AS OrigShipDateRequested
         , ts.NewShipDateRequested                                                                                     AS NewShipDateRequested
         , ISNULL (sl.modifieddatetime, GETDATE ())                                                                   AS BookDate
         , sl.modifiedby                                                                                              AS ModifiedBy
         , ts._RecID                                                                                                   AS _RecID
         , 1                                                                                                           AS _SourceID

           cast(CURRENT_TIMESTAMP as DATETIME2(6))                                                                      AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                                                      AS _ModifiedDate 
      FROM saleshistory_factstage                 ts
      LEFT JOIN {{ ref('salesline') }}       sl
        ON sl.recid            = ts._RecID
       AND sl.salesstatus       <> 4
      LEFT JOIN saleshistory_facthistory            th
        ON th._RecID            = ts._RecID
      LEFT JOIN {{ ref('salesupdatetype_d') }} st
        ON st.SalesUpdateTypeID = ts.SalesUpdateTypeID 
        WHERE ts._RecID  NOT IN (SELECT _RecID FROM {{ this }})
        UNION
        SELECT 
          , SalesOrderLineKey
         ,SalesStatusKey
         , SalesUpdateTypeKey
         , LegalEntityKey
         ,  DeltaBaseAmount
		, CAST(OrigBaseAmount AS DECIMAL(19,4)) AS OrigBaseAmount
         , NewBaseAmount
         , DeltaBaseAmount_TransCur
		, CAST(OrigBaseAmount_TransCur AS DECIMAL(19,4)) AS OrigBaseAmount_TransCur
         , NewBaseAmount_TransCur
         , DeltaOrderedQuantity
		, CAST(OrigOrderedQuantity AS NUMERIC(20,6)) AS OrigOrderedQuantity
         , NewOrderedQuantity
         ,  DeltaOrderedQuantity_SalesUOM
		 	, CAST(OrigOrderedQuantity_SalesUOM AS NUMERIC(20,6)) AS OrigOrderedQuantity_SalesUOM
         , NewOrderedQuantity_SalesUOM
         , DeltaOrderedQuantity_LB
		 ,CAST(OrigOrderedQuantity_LB AS NUMERIC(20,6)) AS OrigOrderedQuantity_LB
         ,  NewOrderedQuantity_LB



         , DeltaOrderedSalesAmount
		 ,CAST(OrigOrderedSalesAmount AS DECIMAL(19,4)) AS OrigOrderedSalesAmount
         ,  NewOrderedSalesAmount
         ,  DeltaOrderedSalesAmount_TransCur
		 ,CAST(OrigOrderedSalesAmount_TransCur AS DECIMAL(19,4)) AS OrigOrderedSalesAmount_TransCur
         ,  NewOrderedSalesAmount_TransCur
         ,  DeltaShipDaysRequested
         , CAST( OrigShipDateRequested AS DATE) AS OrigShipDateRequested
         , NewShipDateRequested
         ,  BookDate
         ,  ModifiedBy
         ,  _RecID
         , _SourceID                                                                                                           AS _SourceID
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
		FROM {{ this }}
		) t;
