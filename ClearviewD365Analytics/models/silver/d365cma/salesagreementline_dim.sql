{{ config(materialized='table', tags=['silver']) }}

-- Source file: cma/cma/layers/_base/_silver/salesagreementline/salesagreementline.py
-- Root method: Salesagreementline.salesagreementlinedetail [SalesAgreementLineDetail]
-- external_table_name: SalesAgreementLineDetail
-- schema_name: temp

SELECT * FROM (
        SELECT
        ROW_NUMBER() OVER (ORDER BY t._RecID) AS SalesAgreementLineKey
          ,CURRENT_TIMESTAMP                                               AS _CreatedDate
        , CURRENT_TIMESTAMP                                               AS _ModifiedDate
        ,'1900-01-01'                                                     AS ActivityDate
        , *  FROM ( SELECT DISTINCT
          sah.customerdataareaid                                               AS LegalEntityID
        , sah.salesnumbersequence                                              AS SalesAgreementID
        , RIGHT('000' + CAST(CAST(al.linenumber AS BIGINT) AS VARCHAR(6)), 6) AS LineNumber
        , ac.name                                                             AS AgreementClassification
        , we.enumvalue                                                     AS AgreementStatus
        , al.cmaexternalitemid                                                AS CustomerPartNumber
        , al.recid                                                            AS _RecID
        , 1                                                                   AS _SourceID
        , al.isdeleted
      FROM {{ ref('agreementline') }}        al
    INNER JOIN {{ ref('agreementheader') }} ah
        ON ah.recid               = al.agreement
     INNER JOIN {{ ref('salesagreementheader') }} sah
        ON sah.recid = ah.recid   
      AND sah.salesnumbersequence <> ''
      LEFT JOIN {{ ref('agreementclassification') }} ac
        ON ac.recid               = ah.agreementclassification
       LEFT JOIN {{ ref('enumeration') }}             we
        ON we.enum                = 'agreementstate'
      AND we.enumvalueid         = ah.agreementstate
      ) t ) p WHERE p.IsDeleted = 0
