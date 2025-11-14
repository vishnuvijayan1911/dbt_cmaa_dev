{{ config(materialized='view', schema='gold', alias="Sales person") }}

SELECT  t.SalesPersonKey                  AS [Sales person key]
    , NULLIF(t.SalesPersonID, '')       AS [Sales person]
    , NULLIF(t.SalesPerson, '')         AS [Sales person name]
    , NULLIF(t.SalesPersonInitials, '') AS [Sales person initials]
  FROM {{ ref("SalesPerson") }} t ;
