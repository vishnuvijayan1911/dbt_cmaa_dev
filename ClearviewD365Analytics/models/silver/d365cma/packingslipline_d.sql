{{ config(materialized='table', tags=['silver'], alias='packingslipline_dim') }}

-- Source file: cma/cma/layers/_base/_silver/packingslipline/packingslipline.py
-- Root method: Packingslipline.packingsliplinedetail [PackingSlipLineDetail]
-- external_table_name: PackingSlipLineDetail
-- schema_name: temp

SELECT  ROW_NUMBER() OVER (ORDER BY t._RecID) AS PackingSlipLineKey
             ,CURRENT_TIMESTAMP                                               AS _CreatedDate
        , CURRENT_TIMESTAMP                                               AS _ModifiedDate
        , '1900-01-01'                                                    AS ActivityDate
        , *
        FROM (
        SELECT DISTINCT
          cpst.dataareaid                                                   AS LegalEntityID
         , cpst.packingslipid                                                 AS PackingSlipID
         , RIGHT('000' + CAST(CAST(cpst.linenum AS BIGINT) AS VARCHAR(6)), 6) AS LineNumber
         , tms.carriercode                                                    AS ShippingCarrier
         , cpst.recid                                                        AS _RecID
         , 1                                                                  AS _SourceID

      FROM {{ ref('custpackingsliptrans') }}     cpst
     INNER JOIN {{ ref('custpackingslipjour') }} cpsj
        ON cpsj.dataareaid   = cpst.dataareaid
       AND cpsj.salesid       = cpst.salesid
       AND cpsj.packingslipid = cpst.packingslipid
       AND cpsj.deliverydate  = cpst.deliverydate
      LEFT JOIN {{ ref('tmscarrierservice') }}   tms
        ON tms.dataareaid    = cpsj.dataareaid
       AND tms.dlvmodeid      = cpsj.dlvmode) t;
