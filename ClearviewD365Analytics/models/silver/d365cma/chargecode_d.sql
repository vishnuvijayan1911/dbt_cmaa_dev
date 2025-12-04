{{ config(materialized='table', tags=['silver'], alias='chargecode') }}

-- Source file: cma/cma/layers/_base/_silver/chargecode/chargecode.py
-- Root method: Chargecode.chargecodedetail [ChargeCodeDetail]
-- Inlined methods: Chargecode.chargecodedetail1 [ChargeCodeDetail1]
-- external_table_name: ChargeCodeDetail
-- schema_name: temp

WITH
chargecodedetail1 AS (
    SELECT DISTINCT
               mu.dataareaid  AS LegalEntityID
             , mu.markupcode  AS ChargeCode
             , mu.txt         AS Charge
             , mu.moduletype  AS ModuleType

          FROM {{ ref('markuptable') }} mu
)
SELECT  {{ dbt_utils.generate_surrogate_key(['t.LegalEntityID', 't.ChargeCode', 't.ModuleTypeID']) }} AS ChargeCodeKey
        , * FROM (
        SELECT DISTINCT
          ts.LegalEntityID                                               AS LegalEntityID
         , ts.ChargeCode                                                  AS ChargeCode
         , CASE WHEN ts.Charge = '' THEN ts.ChargeCode ELSE ts.Charge END AS Charge
         , ISNULL(we1.enumvalueid, '')                                    AS ModuleTypeID
         , we1.enumvalue                                                  AS ModuleType


        ,cast(CURRENT_TIMESTAMP as DATETIME2(6))                                               AS _CreatedDate
        , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                               AS _ModifiedDate
      FROM chargecodedetail1             ts
      LEFT JOIN {{ ref('enumeration') }} we1
        ON we1.enumvalueid = ts.ModuleType
       AND we1.enum        = 'markupmoduletype'
     WHERE ts.ChargeCode <> '') t;

