{{ config(materialized='table', tags=['silver'], alias='forecastmodel') }}

-- Source file: cma/cma/layers/_base/_silver/forecastmodel/forecastmodel.py
-- Root method: Forecastmodel.forecastmodeldetail [ForecastModelDetail]
-- Inlined methods: Forecastmodel.forecastmodelstage [ForecastModelStage]
-- external_table_name: ForecastModelDetail
-- schema_name: temp

WITH
forecastmodelstage AS (
    SELECT DISTINCT
               fm.dataareaid    AS LegalEntityID
             , fm.modelid        AS ModelID
             , dfm.modelid       AS ParentModelID
             , fm.txt            AS Model
             , fm.type           AS ModelTypeID
             , fm.projbudgettype AS BudgetTypeID
          FROM {{ ref('forecastmodel') }}      fm
          LEFT JOIN {{ ref('forecastmodel') }} dfm
            ON dfm.dataareaid = fm.dataareaid
           AND dfm.submodelid  = fm.submodelid
           AND dfm.type        = 1
         WHERE fm.type = 0;
)
SELECT 
        {{ dbt_utils.generate_surrogate_key(['t.ModelID', 't.ParentModelID', 't.LegalEntityID']) }} AS ForecastModelKey
        , * FROM (
          SELECT DISTINCT  ts.LegalEntityID                                          AS LegalEntityID
         , ts.ModelID                                                AS ModelID
         , ISNULL (ts.ParentModelID, '')                             AS ParentModelID
         , CASE WHEN ts.Model = '' THEN ts.ModelID ELSE ts.Model END AS Model
         , ts.ModelTypeID                                            AS ModelTypeID
         , we1.enumvalue                                             AS ModelType
         , ts.BudgetTypeID                                           AS BudgetTypeID
         , we2.enumvalue                                             AS BudgetType

       ,cast(CURRENT_TIMESTAMP as DATETIME2(6))                                               AS _CreatedDate
        , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                              AS _ModifiedDate
      FROM forecastmodelstage               ts
     INNER JOIN {{ ref('enumeration') }} we1
        ON we1.enum        = 'HeadingSub'
       AND we1.enumvalueid = ts.ModelTypeID
     INNER JOIN {{ ref('enumeration') }} we2
        ON we2.enum        = 'ProjBudgetType'
       AND we2.enumvalueid = ts.BudgetTypeID) t;

