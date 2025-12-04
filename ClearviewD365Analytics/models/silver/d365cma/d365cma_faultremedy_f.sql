{{ config(materialized='table', tags=['silver'], alias='faultremedy_fact') }}

-- Source file: cma/cma/layers/_base/_silver/faultremedy_f/faultremedy_f.py
-- Root method: FaultremedyFact.faultremedy_factdetail [FaultRemedy_FactDetail]
-- Inlined methods: FaultremedyFact.faultremedy_factstage [FaultRemedy_FactStage]
-- external_table_name: FaultRemedy_FactDetail
-- schema_name: temp

WITH
faultremedy_factstage AS (
    SELECT ofr.dataareaid        AS LegalEntityID
             , ofc.objectfaultsymptom AS OBJECTFAULTSYMPTOM
             , fc.faultcauseid        AS FaultCauseID
             , fr.faultremedyid       AS FaultRemedyID
             , ofr.recid             AS _RecID
             , 1                      AS _SourceID

          FROM {{ ref('entassetobjectfaultremedy') }}     ofr
          LEFT JOIN {{ ref('entassetobjectfaultcause') }} ofc
            ON ofc.dataareaid = ofr.dataareaid
           AND ofc.recid      = ofr.objectfaultcause
         INNER JOIN {{ ref('entassetfaultcause') }}       fc
            ON fc.dataareaid  = ofr.dataareaid
           AND fc.recid       = ofc.faultcause
          LEFT JOIN {{ ref('entassetfaultremedy') }}      fr
            ON fr.dataareaid  = ofr.dataareaid
           AND fr.recid       = ofr.faultremedy;
)
SELECT {{ dbt_utils.generate_surrogate_key(['stg._RecID', 'stg._SourceID']) }} AS FaultRemedyFactKey
          , fr.faultremedykey   AS FaultRemedyKey
         , f.FaultCauseFactKey AS FactCauseFactKey
         , le.LegalEntityKey   AS LegalEntityKey
         , stg._SourceID       AS _SourceID
         , stg._RecID

      FROM faultremedy_factstage                   stg
     INNER JOIN {{ ref('d365cma_legalentity_d') }}     le
        ON le.LegalEntityID  = stg.LegalEntityID
       AND le._SourceID      = stg._SourceID
      LEFT JOIN {{ ref('d365cma_faultcause_d') }}      ofc
        ON ofc.faultcauseid  = stg.FaultCauseID
       AND ofc.legalentityid = le.LegalEntityID
       AND ofc._sourceid     = stg._SourceID
      LEFT JOIN {{ ref('d365cma_faultremedy_d') }}     fr
        ON fr.faultremedyid  = stg.FaultRemedyID
       AND fr.legalentityid  = le.LegalEntityID
       AND fr._sourceid      = le._SourceID
      LEFT JOIN {{ ref('d365cma_fault_f') }}      ff
        ON ff._RecID         = stg.OBJECTFAULTSYMPTOM
       AND ff._SourceID      = stg._SourceID
      LEFT JOIN {{ ref('d365cma_faultcause_f') }} f
        ON f.FaultCauseKey   = ofc.faultcausekey
       AND f.FaultKey        = ff.FaultKey;
