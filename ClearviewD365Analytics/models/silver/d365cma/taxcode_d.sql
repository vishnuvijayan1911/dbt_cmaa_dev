{{ config(materialized='table', tags=['silver'], alias='taxcode_dim') }}

-- Source file: cma/cma/layers/_base/_silver/taxcode/taxcode.py
-- Root method: Taxcode.taxcodedetail [TaxCodeDetail]
-- Inlined methods: Taxcode.taxcodestage [TaxCodeStage]
-- external_table_name: TaxCodeDetail
-- schema_name: temp

WITH
taxcodestage AS (
    SELECT tt.taxcode                      AS TaxCode
             , tt.taxname                      AS TaxName
             , tt.dataareaid                   AS LegalEntityID
             , td.taxfromdate                  AS TaxStartDate
             , td.taxtodate                    AS TaxEndDate
             , td.taxvalue                     AS TaxValue
             , CAST(tt.taxbase AS VARCHAR(20)) AS TaxBaseID
             , tt.recid                        AS _RecID
             , 1                               AS _SourceID

          FROM {{ ref('taxtable') }}     tt
          LEFT JOIN {{ ref('taxdata') }} td
            ON tt.dataareaid  = td.dataareaid 
           AND tt.taxcode     = td.taxcode;
)
SELECT ROW_NUMBER() OVER (ORDER BY tt._recid) AS TaxCodeKey
         , tt.taxcode       AS TaxCode
         , tt.taxname       AS TaxName
         , tt.legalentityid AS LegalEntityID
         , tt.taxstartdate  AS TaxStartDate
         , tt.taxenddate    AS TaxEndDate
         , tt.taxvalue      AS TaxValue
         , we.enumvalue     AS TaxOrigin
         , tt._recid        AS _RecID
         , 1                AS _SourceID
         , CURRENT_TIMESTAMP                                                            AS _CreatedDate
         , CURRENT_TIMESTAMP                                                            AS _ModifiedDate   

      FROM taxcodestage               tt
     INNER JOIN {{ ref('enumeration') }} we
        ON we.enumvalueid = TaxBaseID
       AND we.enum        = 'TaxBaseType';
