{{ config(materialized='view', schema='gold', alias="Sales history fact") }}

WITH cte
  AS (
    SELECT  sol.SalesOrderLineKey
          , MAX(dc.CustomerKey) AS CustomerKey
        FROM {{ ref("PackingSlipLine_Fact") }} ps
        INNER JOIN {{ ref("SalesOrderLine") }}  sol
          ON sol.SalesOrderLineKey = ps.SalesOrderLineKey
        INNER JOIN {{ ref("Customer") }}        dc
          ON dc.CustomerKey        = ps.CustomerKey
        GROUP BY sol.SalesOrderLineKey)
    SELECT   
          sh.SalesHistoryKey                  AS [Sales history key]
        , sh.SalesOrderLineKey                AS [Sales order line key]
        , sh.LegalEntityKey                   AS [Legal entity key]
        , ISNULL(dd.DateKey, -1)              AS [Book date key]
        , ISNULL(c.CustomerKey, -1)           AS [Customer key]
        , solf.ProductKey                     AS [Product key]
        , stu.SalesUpdateType                 AS [Sales update type]
        , sh.DeltaBaseAmount                  AS [Book base amount]
        , sh.OrigBaseAmount                   AS [Orig base amount]
        , sh.NewBaseAmount                    AS [New base amount]
        , sh.DeltaBaseAmount_TransCur         AS [Book base amount in trans currency]
        , sh.OrigBaseAmount_TransCur          AS [Orig base amount in trans currency]
        , sh.NewBaseAmount_TransCur           AS [New base amount in trans currency]
        , sh.DeltaOrderedQuantity_SalesUOM    AS [Book order quantity]
        , sh.OrigOrderedQuantity_SalesUOM     AS [Orig order quantity]
        , sh.NewOrderedQuantity_SalesUOM      AS [New order quantity]
        , sh.DeltaOrderedQuantity_LB * 1 AS [Book order LB], sh.DeltaOrderedQuantity_LB * 0.01 AS [Book order CWT], sh.DeltaOrderedQuantity_LB * 0.0005 AS [Book order TON]
        , sh.OrigOrderedQuantity_LB * 1 AS [Orig order LB], sh.OrigOrderedQuantity_LB * 0.01 AS [Orig order CWT], sh.OrigOrderedQuantity_LB * 0.0005 AS [Orig order TON]
        , sh.NewOrderedQuantity_LB * 1 AS [New order LB], sh.NewOrderedQuantity_LB * 0.01 AS [New order CWT], sh.NewOrderedQuantity_LB * 0.0005 AS [New order TON]
        , sh.DeltaOrderedSalesAmount          AS [Book sales]
        , sh.OrigOrderedSalesAmount           AS [Orig order sales]
        , sh.NewOrderedSalesAmount            AS [New order sales]
        , sh.DeltaOrderedSalesAmount_TransCur AS [Book sales in trans currency]
        , sh.OrigOrderedSalesAmount_TransCur  AS [Orig order sales in trans currency]
        , sh.NewOrderedSalesAmount_TransCur   AS [New order sales in trans currency]
        , sh.BookDate                         AS [Book date]
        , sh.DeltaShipDaysRequested           AS [Ship requested days change]
        , sh.OrigShipDateRequested            AS [Orig ship requested date]
        , sh.NewShipDateRequested             AS [New ship requested date]
        , sh.ModifiedBy                       AS [Modified by]
      FROM {{ ref("SalesHistory_Fact") }}        sh 
      LEFT JOIN {{ ref("SalesOrderLine_Fact") }} solf
        ON solf.SalesOrderLineKey = sh.SalesOrderLineKey
      LEFT JOIN {{ ref("Date") }}                dd
        ON dd.Date                = CAST(sh.BookDate AS DATE)
      LEFT JOIN cte                     c
        ON c.SalesOrderLineKey    = sh.SalesOrderLineKey	
      LEFT JOIN {{ ref("SalesUpdateType") }}     stu
        ON stu.SalesUpdateTypeKey = sh.SalesUpdateTypeKey;
