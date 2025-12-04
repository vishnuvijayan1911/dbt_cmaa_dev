{{ config(materialized='view', schema='gold', alias="Work order line") }}

SELECT  f.WorkOrderLineKey              AS [Worker order line key]
  , NULLIF(jtc.CertificateType, '') AS [Certificate type]
  , NULLIF(jt.JobTrade, '')        AS [Job trade]
  , f.LineNumber                    AS [Line #]
FROM {{ ref("d365cma_workorderline_f") }}                  f
LEFT JOIN {{ ref("d365cma_maintenancejobtradecertificate_d") }} jtc 
  ON jtc.MaintenanceJobTradeCertificateKey = f.MaintenanceJobTradeCertificateKey
LEFT JOIN {{ ref("d365cma_maintenancejobtrade_d") }} jt
ON jt.MaintenanceJobTradeKey = f.MaintenanceJobTradeKey;
