{{ config(materialized='table', tags=['silver'], alias='salesforecastmodel') }}

-- Source file: cma/cma/layers/_base/_silver/salesforecastmodel/salesforecastmodel.py
-- Root method: Salesforecastmodel.salesforecastmodeldetail [SalesForecastModelDetail]
-- Inlined methods: Salesforecastmodel.salesforecastmodelstage [SalesForecastModelStage]
-- external_table_name: SalesForecastModelDetail
-- schema_name: temp

WITH
salesforecastmodelstage AS (
    SELECT DISTINCT
               fm.dataareaid    AS LegalEntityID
             , fm.modelid        AS ModelID
             , fm.submodelid     AS SubModelID
             , fm.txt            AS Model
             , fm.type           AS ModelTypeID
             , fm.projbudgettype AS BudgetTypeID

          FROM {{ ref('forecastmodel') }} fm
         WHERE EXISTS (   SELECT 1
                            FROM {{ ref('forecastsales') }} fs
                           WHERE fs.dataareaid = fm.dataareaid
                             AND (fs.modelid    = fm.modelid OR fs.modelid = fm.submodelid)
                             AND fs.active      = 1);
)
SELECT 
           {{ dbt_utils.generate_surrogate_key(['t.ModelID']) }} AS SalesForecastModelKey
        , * FROM ( SELECT DISTINCT
           ts.LegalEntityID AS LegalEntityID
         , ts.ModelID       AS ModelID
         , ts.SubModelID    AS SubModelID
         , ts.Model         AS Model
         , ts.ModelTypeID   AS ModelTypeID
         , we1.enumvalue    AS ModelType
         , ts.BudgetTypeID  AS BudgetTypeID
         , we2.enumvalue    AS BudgetType


         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
      FROM salesforecastmodelstage               ts
     INNER JOIN {{ ref('enumeration') }} we1
        ON we1.enum        = 'HeadingSub'
       AND we1.enumvalueid = ts.ModelTypeID
     INNER JOIN {{ ref('enumeration') }} we2
        ON we2.enum        = 'ProjBudgetType'
       AND we2.enumvalueid = ts.BudgetTypeID) t;

