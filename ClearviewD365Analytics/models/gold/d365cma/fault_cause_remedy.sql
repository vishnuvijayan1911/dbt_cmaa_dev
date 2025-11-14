{{ config(materialized='view', schema='gold', alias="Fault cause remedy") }}

SELECT  
  DISTINCT
    CAST(ISNULL(NULLIF(CAST(
      RIGHT('000000000' + CONVERT(VARCHAR(9), ISNULL(dc.FaultCauseKey,'0')), 9) +
      RIGHT('000000000' + CONVERT(VARCHAR(9), ISNULL(dr.FaultRemedyKey,'0')), 9) 
      AS BIGINT),0), -1) AS BIGINT) AS [Fault cause remedy key] 
  , NULLIF(dc.FaultCauseID, '')         AS [Fault cause]
  , NULLIF(dr.FaultRemedyID, '')        AS [Fault remedy]
FROM {{ ref("Fault_Fact") }} f  
LEFT JOIN {{ ref("FaultCause_Fact") }} fc  
  ON fc.FaultKey = f.FaultKey
LEFT JOIN {{ ref("FaultCause") }} dc  
  ON dc.FaultCauseKey = fc.FaultCauseKey
LEFT JOIN {{ ref("FaultRemedy_Fact") }} fr 
  ON fr.FactCauseFactKey = fc.FaultCauseFactKey
LEFT JOIN {{ ref("FaultRemedy") }} dr 
  ON dr.FaultRemedyKey = fr.FaultRemedyKey;
