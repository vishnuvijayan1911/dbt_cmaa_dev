{{ config(materialized='view', schema='gold', alias="Fault cause remedy") }}

SELECT  
  DISTINCT
    CAST(ISNULL(NULLIF(CAST(
      RIGHT('000000000' + CONVERT(VARCHAR(9), ISNULL(dc.FaultCauseKey,'0')), 9) +
      RIGHT('000000000' + CONVERT(VARCHAR(9), ISNULL(dr.FaultRemedyKey,'0')), 9) 
      AS BIGINT),0), -1) AS BIGINT) AS [Fault cause remedy key] 
  , NULLIF(dc.FaultCauseID, '')         AS [Fault cause]
  , NULLIF(dr.FaultRemedyID, '')        AS [Fault remedy]
FROM {{ ref("d365cma_fault_f") }} f  
LEFT JOIN {{ ref("d365cma_faultcause_f") }} fc  
  ON fc.FaultKey = f.FaultKey
LEFT JOIN {{ ref("d365cma_faultcause_d") }} dc  
  ON dc.FaultCauseKey = fc.FaultCauseKey
LEFT JOIN {{ ref("d365cma_faultremedy_f") }} fr 
  ON fr.FactCauseFactKey = fc.FaultCauseFactKey
LEFT JOIN {{ ref("d365cma_faultremedy_d") }} dr 
  ON dr.FaultRemedyKey = fr.FaultRemedyKey;
