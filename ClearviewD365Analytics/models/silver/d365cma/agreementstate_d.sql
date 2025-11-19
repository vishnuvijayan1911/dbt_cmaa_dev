{{ config(materialized='table', tags=['silver'], alias='agreementstate') }}

-- Source file: cma/cma/layers/_base/_silver/agreementstate/agreementstate.py
-- Root method: Agreementstate.agreementstatedetail [AgreementStateDetail]
-- Inlined methods: Agreementstate.agreementstatestage [AgreementStateStage]
-- external_table_name: AgreementStateDetail
-- schema_name: temp

WITH
agreementstatestage AS (
    SELECT DISTINCT
               ah.agreementstate     AS AgreementStateID
             , 1                     AS _SourceID

          FROM {{ ref("agreementheader") }} ah
)
SELECT 
      {{ dbt_utils.generate_surrogate_key(["AgreementStateID"]) }} as AgreementStateKey, 
    ts.agreementatateid AS AgreementStateID
         , we.enumvalue        AS AgreementState
         , ts._sourceid        AS _SourceID
        , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                               AS _ModifiedDate

      FROM agreementstatestage               ts
      LEFT JOIN {{ ref("enumeration") }} we
        ON we.EnumValueID = ts.AgreementStateID
       AND we.Enum        = 'AgreementState';
