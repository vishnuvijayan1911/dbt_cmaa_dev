{{ config(materialized='view', schema='gold', alias="Fault cause remedy") }}

SELECT  
  DISTINCT
    CAST(ISNULL(NULLIF(CAST(
      RIGHT('000000000' + CONVERT(VARCHAR(9), ISNULL(dc.FaultCauseKey,'0')), 9) +
      RIGHT('000000000' + CONVERT(VARCHAR(9), ISNULL(dr.FaultRemedyKey,'0')), 9) 
      AS BIGINT),0), -1) AS BIGINT) AS [Fault cause remedy key] 
  , NULLIF(dc.FaultCauseID, '')         AS [Fault cause]
  , NULLIF(dr.FaultRemedyID, '')        AS [Fault remedy]
FROM {{ ref("fault_fact") }} f  
LEFT JOIN {{ ref("faultcause_fact") }} fc  
  ON fc.FaultKey = f.FaultKey
LEFT JOIN {{ ref("faultcause") }} dc  
  ON dc.FaultCauseKey = fc.FaultCauseKey
LEFT JOIN {{ ref("faultremedy_fact") }} fr 
  ON fr.FactCauseFactKey = fc.FaultCauseFactKey
LEFT JOIN {{ ref("faultremedy") }} dr 
  ON dr.FaultRemedyKey = fr.FaultRemedyKey;
