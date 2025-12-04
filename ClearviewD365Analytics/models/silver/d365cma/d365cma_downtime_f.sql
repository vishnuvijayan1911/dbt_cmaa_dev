{{ config(materialized='table', tags=['silver'], alias='downtime_fact') }}

-- Source file: cma/cma/layers/_base/_silver/downtime_f/downtime_f.py
-- Root method: DowntimeFact.downtime_factdetail [Downtime_FactDetail]
-- Inlined methods: DowntimeFact.downtime_factstage [Downtime_FactStage]
-- external_table_name: Downtime_FactDetail
-- schema_name: temp

WITH
downtime_factstage AS (
    SELECT rt.dataareaid        AS LegalEntityID
             , rt.functionallocation AS RECID_FL
             , rt.object             AS RECID_OT
             , rt.productionstoptype AS RECID_PT
             , rt.workorder          AS RECID_WO
             , rt.stopduration       AS DownTimeDuration
             , rt.stopfrom           AS StartDateTime
             , rt.stopto             AS EndDateTime
             , rt.recid             AS _RecID
             , 1                     AS _SourceID

          FROM {{ ref('entassetobjectproductionstop') }} rt;
)
SELECT {{ dbt_utils.generate_surrogate_key(['ts._RecID', 'ts._SourceID']) }} AS DowntimeKey
          ,dt.DowntimeTypeKey
         , dfl.AssetFunctionalLocationKey AS FunctionalLocationKey
         , do.AssetKey
         , dle.LegalEntityKey
         , sd.DateKey                     AS StartDateKey
         , wd.WorkOrderKey
         , ts.StartDateTime
         , ts.EndDateTime
         , ts.DownTimeDuration
         , ts._RecID
         , ts._SourceID

         ,  cast(CURRENT_TIMESTAMP as DATETIME2(6))    AS  _CreatedDate
         ,  cast(CURRENT_TIMESTAMP as DATETIME2(6))    AS  _ModifiedDate
      FROM downtime_factstage                              ts
      LEFT JOIN {{ ref('d365cma_downtimetype_d') }} dt
        ON dt._RecID         = ts.RECID_PT
       AND dt._SourceID      = 1
     INNER JOIN {{ ref('d365cma_legalentity_d') }}                  dle
        ON dle.LegalEntityID = ts.LegalEntityID
      LEFT JOIN {{ ref('d365cma_asset_d') }}                        do
        ON do._RecID         = ts.RECID_OT
       AND do._SourceID      = 1
      LEFT JOIN {{ ref('d365cma_assetfunctionallocation_d') }}      dfl
        ON dfl._RecID        = ts.RECID_FL
       AND dfl._SourceID     = 1
      LEFT JOIN {{ ref('d365cma_workorder_d') }}                    wd
        ON wd._RecID         = ts.RECID_WO
       AND wd._SourceID      = 1
      LEFT JOIN {{ ref('d365cma_date_d') }}                         sd
        ON sd.Date           = CONVERT (DATE, ts.StartDateTime);
