{{ config(materialized='view', schema='gold', alias="Fault cause remedy bridge") }}

SELECT 
  f.FaultKey                          AS [Fault key]
, CAST(ISNULL(NULLIF(CAST(
    RIGHT('000000000' + CONVERT(VARCHAR(9), ISNULL(fc.FaultCauseKey,'0')), 9) +
    RIGHT('000000000' + CONVERT(VARCHAR(9), ISNULL(fr.FaultRemedyKey,'0')), 9) 
    AS BIGINT),0), -1) AS BIGINT) AS [Fault cause remedy key]
, fc.FaultCauseFactKey                AS [Fault cause fact key] 
, fc.FaultCauseKey                    AS [Fault cause key]
, fr.FaultRemedyFactKey               AS [Fault remedy fact key]
, fr.FaultRemedyKey                   AS [Fault remedy key]
FROM {{ ref("fault_f") }}	            f
LEFT JOIN {{ ref("faultcause_f") }}   fc
ON fc.FaultKey = f.FaultKey
LEFT JOIN {{ ref("faultremedy_f") }}  fr
ON fr.FactCauseFactKey = fc.FaultCauseFactKey;
