{{ config(materialized='table', tags=['silver'], alias='qualityorder_dim') }}

-- Source file: cma/cma/layers/_base/_silver/qualityorder/qualityorder.py
-- Root method: Qualityorder.qualityorderdetail [QualityOrderDetail]
-- external_table_name: QualityOrderDetail
-- schema_name: temp

SELECT ROW_NUMBER () OVER (ORDER BY qt.dataareaid, qt.qualityorderid) AS QualityOrderKey
     , qt.qualityorderid                                              AS QualityOrderID
     , qt.dataareaid                                                  AS LegalEntityID
     , we1.enumvalue                                                  AS OrderStatus
     , qt.inventrefid                                                 AS ReferenceID
     , we2.enumvalue                                                  AS ReferenceType
     , qt.testgroupid                                                 AS TestGroupID
     , qt.createdby                                                   AS CreatedBy
     , qt.modifiedby                                                  AS ModifiedBy
     , qt.recid                                                       AS _RecID
     , 1                                                              AS _SourceID
  FROM {{ ref('inventqualityordertable') }} qt
  LEFT JOIN {{ ref('enumeration') }}        we1
    ON we1.enum        = 'InventTestOrderStatus'
   AND we1.enumvalueid = qt.orderstatus
  LEFT JOIN {{ ref('enumeration') }}        we2
    ON we2.enum        = 'InventTestReferenceType'
   AND we2.enumvalueid = qt.referencetype;
