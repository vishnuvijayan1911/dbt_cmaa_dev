{{ config(materialized='table', tags=['silver'], alias='purchaseagreementline') }}

-- Source file: cma/cma/layers/_base/_silver/purchaseagreementline/purchaseagreementline.py
-- Root method: Purchaseagreementline.purchaseagreementlinedetail [PurchaseAgreementLineDetail]
-- external_table_name: PurchaseAgreementLineDetail
-- schema_name: temp

SELECT 
        ROW_NUMBER() OVER (ORDER BY t._RecID) AS PurchaseAgreementLineKey
        ,'1900-01-01'                                                         AS ActivityDate
        , * FROM ( SELECT DISTINCT
           pah.vendordataareaid                                                 AS LegalEntityID
        , pah.purchnumbersequence                                              AS PurchaseAgreementID
        , RIGHT('000' + CAST(CAST(al.linenumber AS BIGINT) AS VARCHAR(6)), 6) AS LineNumber
        , ac.name                                                              AS AgreementClassification
        , ah.documenttitle                                                     AS DocumentTitle
        , al.recid                                                           AS _RecID
        , 1                                                                   AS _SourceID

        ,  cast(CURRENT_TIMESTAMP as DATETIME2(6))                                                                                                      AS  _CreatedDate
        ,  cast(CURRENT_TIMESTAMP as DATETIME2(6))                                                                                                      AS  _ModifiedDate
      FROM {{ ref('agreementline') }}             al
      INNER JOIN {{ ref('agreementheader') }} ah
       ON ah.recid        = al.agreement
      LEFT JOIN {{ ref('purchagreementheader') }} pah
        ON pah.recid = ah.recid
        AND pah.purchnumbersequence <> ''
       LEFT OUTER JOIN {{ ref('dimensionattributevalueset') }} T2 
       ON(( ah.defaultdimension  =  T2.recid)  
       AND ( ah.partition  =  T2.partition)) 
       LEFT JOIN {{ ref('agreementclassification') }} ac
        ON ac.recid        = ah.agreementclassification
    WHERE ah.instancerelationtype IN( 6827)) t;

