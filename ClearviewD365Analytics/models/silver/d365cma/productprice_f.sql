{{ config(materialized='table', tags=['silver'], alias='productprice_fact') }}

-- Source file: cma/cma/layers/_base/_silver/productprice_f/productprice_f.py
-- Root method: ProductpriceFact.productprice_factdetail [ProductPrice_FactDetail]
-- Inlined methods: ProductpriceFact.productprice_factcost [ProductPrice_FactCost]
-- external_table_name: ProductPrice_FactDetail
-- schema_name: temp

WITH
productprice_factcost AS (
    SELECT IM.dataareaid
             , IM.itemid
             , CAST(ISNULL(
                        IM.price / CASE WHEN ISNULL(IM.priceunit, 0) = 0 THEN 1 ELSE IM.priceunit END + IM.markup
                        / CASE WHEN ISNULL(IM.priceqty, 0) = 0 THEN 1 ELSE IM.priceqty END
                      , 0) AS DECIMAL(18, 8)) AS StandardCost
             , IM.activationdate              AS FromDate
             , ISNULL(
                   DATEADD(DAY, -1, LEAD(IM.activationdate) OVER (PARTITION BY IM.itemid ORDER BY IM.activationdate))
                 , '9999-12-31')              AS ToDate

          FROM {{ ref('inventitemprice') }} IM
         WHERE IM.pricetype   = 0
           AND IM.costingtype = 2;
)
SELECT 
         , {{ dbt_utils.generate_surrogate_key(['dd.DateKey']) }} AS ProductPriceKey
         , dle.LegalEntityKey
         , dp.ProductKey
         , tc.StandardCost AS StandardPrice
         , dd.DateKey      AS FromDateKey
         , dd1.DateKey     AS ToDateKey

         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
      FROM productprice_factcost      tc
     INNER JOIN {{ ref('legalentity_d') }} dle
        ON dle.LegalEntityID = tc.DATAAREAID
     INNER JOIN {{ ref('product_d') }}     dp
        ON dp.LegalEntityID  = dle.LegalEntityID
       AND dp.ItemID         = tc.ITEMID
     INNER JOIN {{ ref('date_d') }}        dd
        ON dd.Date           = tc.FromDate
     INNER JOIN {{ ref('date_d') }}        dd1
        ON dd1.Date          = tc.ToDate;
