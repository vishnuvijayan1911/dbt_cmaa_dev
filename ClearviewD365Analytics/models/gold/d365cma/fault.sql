{{ config(materialized='view', schema='gold', alias="Fault") }}

SELECT  
    t.FaultKey                    AS [Fault key]
  , NULLIF(t.FaultID, '')         AS [Fault #]
  , NULLIF(fa.FaultAreaID, '')    AS [Fault area]
  , NULLIF(fs.FaultSymptomID, '') AS [Fault symptom]
  , NULLIF(ft.FaultTypeID, '')    AS [Fault type]
FROM {{ ref("fault_fact") }} t   
JOIN {{ ref("faultsymptom") }} fs   
    ON fs.FaultSymptomKey = t.FaultSymptomKey
LEFT JOIN {{ ref("faulttype") }} ft   
  ON ft.FaultTypeKey = t.FaultTypeKey
LEFT JOIN {{ ref("faultarea") }} fa   
    ON fa.FaultAreaKey = t.FaultAreaKey;
