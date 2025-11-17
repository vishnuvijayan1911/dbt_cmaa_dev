{{ config(materialized='view', schema='gold', alias="Sales forecast model") }}

SELECT  t.SalesForecastModelKey  AS [Sales forecast model key]
    , NULLIF(t.BudgetType, '') AS [Budget type]
    , NULLIF(t.ModelID, '')    AS [Model]
    , NULLIF(t.Model, '')      AS [Model name]
    , NULLIF(t.ModelType, '')  AS [Model type]
    , NULLIF(t.SubModelID, '') AS [Sub model]
  FROM {{ ref("salesforecastmodel_d") }}  t;
