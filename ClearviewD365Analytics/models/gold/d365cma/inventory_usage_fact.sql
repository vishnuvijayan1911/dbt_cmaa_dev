{{ config(materialized='view', schema='gold', alias="Inventory usage fact") }}

SELECT  pd.DateKey                                                                  AS [Usage date key]
    , it.ProductKey                                                               AS [Product key]
    , it.LegalEntityKey                                                           AS [Legal entity key]
    , CAST(1 AS INT)                                                              AS [Inventory usage count]
    , SUM (CASE WHEN s.InventorySourceID = 0 THEN it.PostedCost END)              AS [Sales usage cost]
    , SUM (CASE WHEN s.InventorySourceID = 0 THEN it.TransQuantity * -1 END)      AS [Sales usage]
    , SUM (CASE WHEN s.InventorySourceID = 0 THEN it.TransQuantity_FT * -1 END) * 1 AS [Sales usage FT], SUM (CASE WHEN s.InventorySourceID = 0 THEN it.TransQuantity_FT * -1 END) * 12 AS [Sales usage IN]
    , SUM (CASE WHEN s.InventorySourceID = 0 THEN it.TransQuantity_PC * -1 END) * 1 AS [Sales usage PC]
    , SUM (CASE WHEN s.InventorySourceID = 0 THEN it.TransQuantity_LB * -1 END) * 1 AS [Sales usage LB], SUM (CASE WHEN s.InventorySourceID = 0 THEN it.TransQuantity_LB * -1 END) * 0.01 AS [Sales usage CWT], SUM (CASE WHEN s.InventorySourceID = 0 THEN it.TransQuantity_LB * -1 END) * 0.0005 AS [Sales usage TON]
    , SUM (CASE WHEN s.InventorySourceID = 0 THEN it.TransQuantity_SQIN * -1 END) * 1 AS [Sales usage SQIN]
    , SUM (CASE WHEN s.InventorySourceID = 0
                  AND ts.InventoryTransStatusID IN ( 1, 2 )
                  AND ts.InventoryTransStatusTypeID = 1
                THEN it.PostedCost END)                                           AS [Shipment usage cost]
    , SUM (CASE WHEN s.InventorySourceID = 0
                  AND ts.InventoryTransStatusID IN ( 1, 2 )
                  AND ts.InventoryTransStatusTypeID = 1
                THEN it.TransQuantity * -1 END)                                   AS [Shipment usage]
    , 
                                    SUM (CASE WHEN s.InventorySourceID = 0
                                      AND ts.InventoryTransStatusID IN ( 1, 2 )
                                      AND ts.InventoryTransStatusTypeID = 1
                                    THEN it.TransQuantity_FT * -1 END)
                                     * 1 AS [Shipment usage FT], 
                                    SUM (CASE WHEN s.InventorySourceID = 0
                                      AND ts.InventoryTransStatusID IN ( 1, 2 )
                                      AND ts.InventoryTransStatusTypeID = 1
                                    THEN it.TransQuantity_FT * -1 END)
                                     * 12 AS [Shipment usage IN]
    , 
                                    SUM (CASE WHEN s.InventorySourceID = 0
                                      AND ts.InventoryTransStatusID IN ( 1, 2 )
                                      AND ts.InventoryTransStatusTypeID = 1
                                    THEN it.TransQuantity_PC * -1 END)
                                     * 1 AS [Shipment usage PC]
      , 
                                SUM (CASE WHEN s.InventorySourceID = 0
                                    AND ts.InventoryTransStatusID IN ( 1, 2 )
                                    AND ts.InventoryTransStatusTypeID = 1
                                  THEN it.TransQuantity_LB * -1 END)
                                 * 1 AS [Shipment usage LB], 
                                SUM (CASE WHEN s.InventorySourceID = 0
                                    AND ts.InventoryTransStatusID IN ( 1, 2 )
                                    AND ts.InventoryTransStatusTypeID = 1
                                  THEN it.TransQuantity_LB * -1 END)
                                 * 0.01 AS [Shipment usage CWT], 
                                SUM (CASE WHEN s.InventorySourceID = 0
                                    AND ts.InventoryTransStatusID IN ( 1, 2 )
                                    AND ts.InventoryTransStatusTypeID = 1
                                  THEN it.TransQuantity_LB * -1 END)
                                 * 0.0005 AS [Shipment usage TON]
      , 
                              SUM (CASE WHEN s.InventorySourceID = 0
                                    AND ts.InventoryTransStatusID IN ( 1, 2 )
                                    AND ts.InventoryTransStatusTypeID = 1
                                  THEN it.TransQuantity_SQIN * -1 END)
                                   * 1 AS [Shipment usage SQIN]
    , SUM (CASE WHEN s.InventorySourceID = 8 THEN it.PostedCost END)              AS [Production usage cost]
    , SUM (CASE WHEN s.InventorySourceID = 8 THEN it.TransQuantity * -1 END)      AS [Production usage]
    , SUM (CASE WHEN s.InventorySourceID = 8 THEN it.TransQuantity_FT * -1 END) * 1 AS [Production usage FT], SUM (CASE WHEN s.InventorySourceID = 8 THEN it.TransQuantity_FT * -1 END) * 12 AS [Production usage IN]
    , SUM (CASE WHEN s.InventorySourceID = 8 THEN it.TransQuantity_PC * -1 END) * 1 AS [Production usage PC]
    , SUM (CASE WHEN s.InventorySourceID = 8 THEN it.TransQuantity_LB * -1 END) * 1 AS [Production usage LB], SUM (CASE WHEN s.InventorySourceID = 8 THEN it.TransQuantity_LB * -1 END) * 0.01 AS [Production usage CWT], SUM (CASE WHEN s.InventorySourceID = 8 THEN it.TransQuantity_LB * -1 END) * 0.0005 AS [Production usage TON]
    , SUM (CASE WHEN s.InventorySourceID = 8 THEN it.TransQuantity_SQIN * -1 END) * 1 AS [Production usage SQIN]
  FROM {{ ref("InventoryTrans_Fact") }}       it
INNER JOIN {{ ref("InventorySource") }}      s
    ON s.InventorySourceKey       = it.InventorySourceKey
INNER JOIN {{ ref("InventoryTransStatus") }} ts
    ON ts.InventoryTransStatusKey = it.InventoryTransStatusKey
INNER JOIN {{ ref("Date") }}                 pd
    ON pd.Date                    = it.DatePhysical
WHERE s.InventorySourceID IN ( 0, 8 )
GROUP BY pd.DateKey
        , it.ProductKey
        , it.LegalEntityKey
HAVING SUM (it.TransQuantity) <> 0
    OR SUM (it.PostedCost)    <> 0;
