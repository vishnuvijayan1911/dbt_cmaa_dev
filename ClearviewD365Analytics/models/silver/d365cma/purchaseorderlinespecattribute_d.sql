{{ config(materialized='table', tags=['silver'], alias='purchaseorderlinespecattribute') }}

-- Source file: cma/cma/layers/_base/_silver/purchaseorderlinespecattribute/purchaseorderlinespecattribute.py
-- Root method: Purchaseorderlinespecattribute.purchaseorderlinespecattributedetail [PurchaseOrderLineSpecAttributeDetail]
-- Inlined methods: Purchaseorderlinespecattribute.purchaseorderlinespecattribute [PurchaseOrderLineSpecAttribute]
-- external_table_name: PurchaseOrderLineSpecAttributeDetail
-- schema_name: temp

WITH
purchaseorderlinespecattribute AS (
    SELECT pl.recid                                                                                       AS RecID
             , ba.pdsbatchattribrelation
             , CASE WHEN ba.pdsbatchattribrelation IN ( 'Grade' )
                    THEN ba.pdsbatchattribtarget
                    ELSE CASE WHEN ISNUMERIC(ba.pdsbatchattribtarget) = 1 THEN ba.pdsbatchattribtarget END END AS PdsBatchAttribTarget
             , CASE WHEN ISNUMERIC(ba.pdsbatchattribmin) = 1 THEN ba.pdsbatchattribmin END                     AS PDSBATCHATTRIBMIN
             , CASE WHEN ISNUMERIC(ba.pdsbatchattribmax) = 1 THEN ba.pdsbatchattribmax END                     AS PDSBATCHATTRIBMAX
             , pl.modifieddatetime                                                                            AS _SourceDate
          FROM {{ ref('purchline') }}               pl
         INNER JOIN {{ ref('cmabatchattributes') }} ba
            ON ba.refrecid = pl.recid
         WHERE ba.cmabarelation = 2;
)
SELECT dso.PurchaseOrderLineKey
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Al' THEN ba.pdsbatchattribtarget END)                         AS Aluminum_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Al' THEN ba.pdsbatchattribmin END)                            AS Aluminum_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Al' THEN ba.pdsbatchattribmax END)                            AS Aluminum_Maximum
         , MAX(CASE WHEN ba.pdsbatchattribrelation IN ( 'CoilWeight', 'CoilWgt' ) THEN ba.pdsbatchattribtarget END) AS CoilWeight_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation IN ( 'CoilWeight', 'CoilWgt' ) THEN ba.pdsbatchattribmin END)    AS CoilWeight_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation IN ( 'CoilWeight', 'CoilWgt' ) THEN ba.pdsbatchattribmax END)    AS CoilWeight_Maximum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'ID' THEN ba.pdsbatchattribtarget END)                         AS InnerDiameter_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'ID' THEN ba.pdsbatchattribmin END)                            AS InnerDiameter_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'ID' THEN ba.pdsbatchattribmax END)                            AS InnerDiameter_Maximum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'OD' THEN ba.pdsbatchattribtarget END)                         AS OuterDiameter_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'OD' THEN ba.pdsbatchattribmin END)                            AS OuterDiameter_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'OD' THEN ba.pdsbatchattribmax END)                            AS OuterDiameter_Maximum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'PIW' THEN ba.pdsbatchattribtarget END)                        AS PIW_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'PIW' THEN ba.pdsbatchattribmin END)                           AS PIW_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'PIW' THEN ba.pdsbatchattribmax END)                           AS PIW_Maximum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Quality' THEN ba.pdsbatchattribtarget END)                    AS Quality_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Quality' THEN ba.pdsbatchattribmin END)                       AS Quality_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Quality' THEN ba.pdsbatchattribmax END)                       AS Quality_Maximum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Thickness' THEN ba.pdsbatchattribtarget END)                  AS Thickness_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Thickness' THEN ba.pdsbatchattribmin END)                     AS Thickness_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Thickness' THEN ba.pdsbatchattribmax END)                     AS Thickness_Maximum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Width' THEN ba.pdsbatchattribtarget END)                      AS Width_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Width' THEN ba.pdsbatchattribmin END)                         AS Width_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Width' THEN ba.pdsbatchattribmax END)                         AS Width_Maximum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Yield' THEN ba.pdsbatchattribtarget END)                      AS Yield_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Yield' THEN ba.pdsbatchattribmin END)                         AS Yield_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Yield' THEN ba.pdsbatchattribmax END)                         AS Yield_Maximum
         , MAX(ba._sourcedate)                                                                                      AS _SourceDate
         , ba.recid                                                                                                AS _RecID
         , 1                                                                                                        AS _SourceID
         ,CURRENT_TIMESTAMP                                               AS _CreatedDate
        , CURRENT_TIMESTAMP                                               AS _ModifiedDate

      FROM purchaseorderlinespecattribute                      ba
     INNER JOIN {{ ref('purchaseorderline_d') }} dso
        ON dso._RecID    = ba.recid
       AND dso._SourceID = 1
     GROUP BY dso.PurchaseOrderLineKey
            , ba.recid;

