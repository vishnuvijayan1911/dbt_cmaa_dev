{{ config(materialized='view', schema='gold', alias="Work order line") }}

SELECT  f.WorkOrderLineKey              AS [Worker order line key]
  , NULLIF(jtc.CertificateType, '') AS [Certificate type]
  , NULLIF(jt.JobTrade, '')        AS [Job trade]
  , f.LineNumber                    AS [Line #]
FROM {{ ref("WorkOrderLine_Fact") }}                  f
LEFT JOIN {{ ref("MaintenanceJobTradeCertificate") }} jtc 
  ON jtc.MaintenanceJobTradeCertificateKey = f.MaintenanceJobTradeCertificateKey
LEFT JOIN {{ ref("MaintenanceJobTrade") }} jt
ON jt.MaintenanceJobTradeKey = f.MaintenanceJobTradeKey;
