{{ config(materialized='table', tags=['silver'], alias='faultcause_fact') }}

-- Source file: cma/cma/layers/_base/_silver/faultcause_f/faultcause_f.py
-- Root method: FaultcauseFact.faultcause_factdetail [FaultCause_FactDetail]
-- Inlined methods: FaultcauseFact.faultcause_factstage [FaultCause_FactStage]
-- external_table_name: FaultCause_FactDetail
-- schema_name: temp

WITH
faultcause_factstage AS (
    SELECT ofs.dataareaid AS LegalEntityID
             , fc.faultcauseid AS FaultCauseID
             , ofc.recid     AS _RecID
             , ofs.recid     AS _RecID1
             , 1               AS _SourceID

         FROM {{ ref('entassetobjectfaultcause') }}        ofc

         INNER JOIN {{ ref('entassetobjectfaultsymptom') }} ofs
            ON ofs.dataareaid = ofc.dataareaid
           AND ofs.recid      = ofc.objectfaultsymptom
          LEFT JOIN {{ ref('entassetfaultcause') }}         fc
            ON fc.dataareaid  = ofc.dataareaid
           AND fc.recid      = ofc.faultcause;
)
SELECT {{ dbt_utils.generate_surrogate_key(['stg._RecID', 'stg._SourceID']) }} AS FaultCauseFactKey
          ,ff.FaultKey       AS FaultKey
         , ofc.faultcausekey AS FaultCauseKey
         , stg._SourceID     AS _SourceID
         , stg._RecID

         ,cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _CreatedDate
         ,cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate
      FROM faultcause_factstage               stg
     INNER JOIN {{ ref('legalentity_d') }} le
        ON le.LegalEntityID  = stg.LegalEntityID
     INNER JOIN {{ ref('fault_f') }}  ff
        ON ff._RecID         = stg._RecID1
       AND ff._SourceID      = stg._SourceID
      LEFT JOIN {{ ref('faultcause_d') }}  ofc
        ON ofc.faultcauseid  = stg.FaultCauseID
       AND ofc.legalentityid = le.LegalEntityID
       AND ofc._sourceid     = stg._SourceID;
