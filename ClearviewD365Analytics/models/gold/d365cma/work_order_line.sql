{{ config(materialized='view', schema='gold', alias="Work order line") }}

SELECT  f.WorkOrderLineKey              AS [Worker order line key]
  , NULLIF(jtc.CertificateType, '') AS [Certificate type]
  , NULLIF(jt.JobTrade, '')        AS [Job trade]
  , f.LineNumber                    AS [Line #]
FROM {{ ref("workorderline_fact") }}                  f
LEFT JOIN {{ ref("maintenancejobtradecertificate") }} jtc 
  ON jtc.MaintenanceJobTradeCertificateKey = f.MaintenanceJobTradeCertificateKey
LEFT JOIN {{ ref("maintenancejobtrade") }} jt
ON jt.MaintenanceJobTradeKey = f.MaintenanceJobTradeKey;
