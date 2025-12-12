{{ config(materialized='table', tags=['silver'], alias='salesorderline') }}

-- Source file: cma/cma/layers/_base/_silver/salesorderline/salesorderline.py
-- Root method: Salesorderline.salesorderlinedetail [SalesOrderLineDetail]
-- external_table_name: SalesOrderLineDetail
-- schema_name: temp

SELECT 
           {{ dbt_utils.generate_surrogate_key(['sl.salesid']) }} AS SalesOrderLineKey
         , sl.dataareaid                                                            AS LegalEntityID
         , sh.salesid                                                               AS SalesOrderID
         , RIGHT('000' + CAST(CAST(sl.customerlinenum AS BIGINT) AS VARCHAR(7)), 7) AS LineNumber
         , CASE WHEN sl.name = '' THEN sh.salesid ELSE sl.name END                  AS SalesOrderLineText
         , sl.externalitemid                                                        AS CustomerPartNumber
		 , sl.inventtransid                                                             AS LotID
         , sh.returnitemnum                                                         AS RMANumber
         , sl.modifieddatetime                                                      AS _SourceDate
         , sl.recid                                                                 AS _RecID
         , 1                                                                        AS _SourceID

         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
      FROM {{ ref('salesline') }}       sl
     INNER JOIN {{ ref('salestable') }} sh
        ON sh.dataareaid  = sl.dataareaid
       AND sh.salesid     = sl.salesid
       AND sh.salesstatus <> 4 
     WHERE sl.salesstatus <> 4 
       AND sl.salestype IN ( 3, 4 );

