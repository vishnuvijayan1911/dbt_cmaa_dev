{{ config(materialized='view', schema='gold', alias="Sales invoice line charge trans fact") }}

SELECT  t.SalesInvoiceLineTransKey                                                           AS [Sales invoice line trans key]
    , SUM (CASE WHEN cc.ChargeCode = 'BEVELING' THEN t.TotalCharges ELSE 0 END)            AS [Beveling Charge]
    , SUM (CASE WHEN cc.ChargeCode = 'BEVELING' THEN t.TotalCharges_TransCur ELSE 0 END)   AS [Beveling Charge in trans currency]
    , SUM (CASE WHEN cc.ChargeCode = 'FREIGHT' THEN t.TotalCharges ELSE 0 END)             AS [Freight charge]
    , SUM (CASE WHEN cc.ChargeCode = 'FREIGHT' THEN t.TotalCharges_TransCur ELSE 0 END)    AS [Freight charge in trans currency]
    , SUM (CASE WHEN cc.ChargeCode = 'FUEL' THEN t.TotalCharges ELSE 0 END)                AS [Fuel charge]
    , SUM (CASE WHEN cc.ChargeCode = 'FUEL' THEN t.TotalCharges_TransCur ELSE 0 END)       AS [Fuel charge in trans currency]
    , SUM (CASE WHEN cc.ChargeCode = 'GRADE ADD' THEN t.TotalCharges ELSE 0 END)           AS [Grade adder]
    , SUM (CASE WHEN cc.ChargeCode = 'GRADE ADD' THEN t.TotalCharges_TransCur ELSE 0 END)  AS [Grade adder in trans currency]
    , SUM (CASE WHEN cc.ChargeCode = 'RESTOCKING' THEN t.TotalCharges ELSE 0 END)          AS [Restocking fee]
    , SUM (CASE WHEN cc.ChargeCode = 'RESTOCKING' THEN t.TotalCharges_TransCur ELSE 0 END) AS [Restocking fee in trans currency]
    , SUM (CASE WHEN cc.ChargeCode NOT IN ( 'BEVELING', 'FREIGHT', 'FUEL', 'GRADE ADD', 'RESTOCKING' )
                THEN t.TotalCharges
                ELSE 0 END)                                                                AS [Unmapped charges]
    , SUM (CASE WHEN cc.ChargeCode NOT IN ( 'BEVELING', 'FREIGHT', 'FUEL', 'GRADE ADD', 'RESTOCKING' )
                THEN t.TotalCharges_TransCur
                ELSE 0 END)                                                                AS [Unmapped charges in trans currency]
  FROM {{ ref("salesinvoicelinechargetrans_f") }} t
  LEFT JOIN {{ ref("salesinvoicelinetrans_f") }}  silt 
    ON silt.SalesInvoiceLineTransKey  = t.SalesInvoiceLineTransKey
  LEFT JOIN {{ ref("salesinvoiceline_f") }}       silf 
    ON silf.SalesInvoiceLineKey       = silt.SalesInvoiceLineKey
  LEFT JOIN {{ ref("salesinvoice_d") }}                si
    ON si.SalesInvoiceKey             = silf.SalesInvoiceKey
  LEFT JOIN {{ ref("salesinvoicelinecharge_f") }} solc 
    ON solc.SalesInvoiceLineChargeKey = t.SalesInvoiceLineChargeKey
  LEFT JOIN {{ ref("chargecode_d") }}                  cc 
    ON cc.ChargeCodeKey               = solc.ChargeCodeKey
GROUP BY t.SalesInvoiceLineTransKey;
