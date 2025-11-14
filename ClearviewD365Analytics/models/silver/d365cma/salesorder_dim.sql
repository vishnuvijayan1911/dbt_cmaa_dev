{{ config(materialized='table', tags=['silver']) }}

-- Source file: cma/cma/layers/_base/_silver/salesorder/salesorder.py
-- Root method: Salesorder.salesorderdetail [SalesOrderDetail]
-- external_table_name: SalesOrderDetail
-- schema_name: temp

SELECT ROW_NUMBER() OVER (ORDER BY st.recid) AS SalesOrderKey
            , st.customerref                                                   AS CustomerReference
            , st.purchorderformnum                                              AS CustomerRequisition
            , st.deliveryname                                                   AS DeliveryName
            , st.dataareaid                                                     AS LegalEntityID
            , CASE WHEN st.mcrorderstopped = 1 THEN 1 ELSE 0 END                AS IsOnHold
            , CASE WHEN st.salesname = '' THEN st.salesid ELSE st.salesname END AS SalesDesc
            , st.salesid                                                        AS SalesOrderID
            , st.returnitemnum                                                  AS RMANumber
            , st.modifieddatetime                                               AS _SourceDate
            , st.recid                                                          AS _RecID
            , 1                                                                 AS _SourceID
            , CURRENT_TIMESTAMP                                                 AS _CreatedDate
            , CURRENT_TIMESTAMP                                                 AS _ModifiedDate
            ,'1900-01-01'                                                       AS ActivityDate  

        FROM {{ ref('salestable') }} st
