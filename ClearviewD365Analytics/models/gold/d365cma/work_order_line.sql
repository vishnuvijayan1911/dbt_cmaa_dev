{{ config(materialized='view', schema='gold', alias="Work order line") }}

SELECT  f.WorkOrderLineKey              AS [Worker order line key]
  , NULLIF(jtc.CertificateType, '') AS [Certificate type]
  , NULLIF(jt.JobTrade, '')        AS [Job trade]
  , f.LineNumber                    AS [Line #]
FROM {{ ref("workorderline_f") }}                  f
LEFT JOIN {{ ref("maintenancejobtradecertificate_d") }} jtc 
  ON jtc.MaintenanceJobTradeCertificateKey = f.MaintenanceJobTradeCertificateKey
LEFT JOIN {{ ref("maintenancejobtrade_d") }} jt
ON jt.MaintenanceJobTradeKey = f.MaintenanceJobTradeKey;
