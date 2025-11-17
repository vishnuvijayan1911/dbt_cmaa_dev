{{ config(materialized='view', schema='gold', alias="Sales status") }}

SELECT 
    t.SalesStatusKey      AS [Sales status key],
    t.SalesStatus         AS [Sales status],
    t.SalesStatusID       AS [Sales status #]
FROM {{ ref("salesstatus") }} t;
