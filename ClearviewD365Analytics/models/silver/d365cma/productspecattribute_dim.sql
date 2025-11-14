{{ config(materialized='table', tags=['silver']) }}

-- Source file: cma/cma/layers/_base/_silver/productspecattribute/productspecattribute.py
-- Root method: Productspecattribute.productspecattributedetail [ProductSpecAttributeDetail]
-- Inlined methods: Productspecattribute.productspecattributestage [ProductSpecAttributeStage]
-- external_table_name: ProductSpecAttributeDetail
-- schema_name: temp

WITH
productspecattributestage AS (
    SELECT ba.dataareaid                                                                                           AS LegalEntityID
             , ba.pdsbatchattribitemid
             , ba.pdsbatchattribrelation
             , CASE WHEN ba.pdsbatchattribrelation IN ( 'Grade' )
                    THEN ba.pdsbatchattribtarget
                    ELSE CASE WHEN ISNUMERIC(ba.pdsbatchattribtarget) = 1 THEN ba.pdsbatchattribtarget ELSE '0' END END AS PDSBATCHATTRIBTARGET
             , CASE WHEN ISNUMERIC(ba.pdsbatchattribmin) = 1 THEN ba.pdsbatchattribmin ELSE '0' END                     AS PDSBATCHATTRIBMIN
             , CASE WHEN ISNUMERIC(ba.pdsbatchattribmax) = 1 THEN ba.pdsbatchattribmax ELSE '0' END                     AS PDSBATCHATTRIBMAX
             , it.recid                                                                                                AS _RecID

          FROM {{ ref('inventtable') }}               it

         INNER JOIN {{ ref('pdsbatchattribbyitem') }} ba
            ON ba.dataareaid          = it.dataareaid
           AND ba.pdsbatchattribitemid = it.itemid
         WHERE ba.pdsbatchattribcode IN ( 0, 1 );
)
SELECT dp.ProductKey
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Grade' THEN ba.pdsbatchattribtarget END)                      AS Grade
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'MaxWt' THEN ba.pdsbatchattribmax END)                         AS MaxWeight
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Length' THEN ba.pdsbatchattribtarget END)                     AS Length_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Length' THEN ba.pdsbatchattribmin END)                        AS Length_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Length' THEN ba.pdsbatchattribmax END)                        AS Length_Maximum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Width' THEN ba.pdsbatchattribtarget END)                      AS Width_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Width' THEN ba.pdsbatchattribmin END)                         AS Width_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Width' THEN ba.pdsbatchattribmax END)                         AS Width_Maximum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'ID' THEN ba.pdsbatchattribtarget END)                         AS InnerDiameter_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'ID' THEN ba.pdsbatchattribmin END)                            AS InnerDiameter_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'ID' THEN ba.pdsbatchattribmax END)                            AS InnerDiameter_Maximum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'OD' THEN ba.pdsbatchattribtarget END)                         AS OuterDiameter_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'OD' THEN ba.pdsbatchattribmin END)                            AS OuterDiameter_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'OD' THEN ba.pdsbatchattribmax END)                            AS OuterDiameter_Maximum
         , MAX(CASE WHEN ba.pdsbatchattribrelation IN ( 'CoilWgt', 'CoilWeight' ) THEN ba.pdsbatchattribtarget END) AS CoilWeight_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation IN ( 'CoilWgt', 'CoilWeight' ) THEN ba.pdsbatchattribmin END)    AS CoilWeight_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation IN ( 'CoilWgt', 'CoilWeight' ) THEN ba.pdsbatchattribmax END)    AS CoilWeight_Maximum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Al' THEN ba.pdsbatchattribtarget END)                         AS Aluminum_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Al' THEN ba.pdsbatchattribmin END)                            AS Aluminum_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Al' THEN ba.pdsbatchattribmax END)                            AS Aluminum_Maximum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'B' THEN ba.pdsbatchattribtarget END)                          AS Boron_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'B' THEN ba.pdsbatchattribmin END)                             AS Boron_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'B' THEN ba.pdsbatchattribmax END)                             AS Boron_Maximum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Brinell' THEN ba.pdsbatchattribtarget END)                    AS Brinell_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Brinell' THEN ba.pdsbatchattribmin END)                       AS Brinell_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Brinell' THEN ba.pdsbatchattribmax END)                       AS Brinell_Maximum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'C' THEN ba.pdsbatchattribtarget END)                          AS Carbon_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'C' THEN ba.pdsbatchattribmin END)                             AS Carbon_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'C' THEN ba.pdsbatchattribmax END)                             AS Carbon_Maximum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Ca' THEN ba.pdsbatchattribtarget END)                         AS Calcium_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Ca' THEN ba.pdsbatchattribmin END)                            AS Calcium_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Ca' THEN ba.pdsbatchattribmax END)                            AS Calcium_Maximum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Cr' THEN ba.pdsbatchattribtarget END)                         AS Chromium_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Cr' THEN ba.pdsbatchattribmin END)                            AS Chromium_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Cr' THEN ba.pdsbatchattribmax END)                            AS Chromium_Maximum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Cu' THEN ba.pdsbatchattribtarget END)                         AS Copper_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Cu' THEN ba.pdsbatchattribmin END)                            AS Copper_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Cu' THEN ba.pdsbatchattribmax END)                            AS Copper_Maximum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'FlakeAttribute' THEN ba.pdsbatchattribtarget END)             AS FlakeAttribute_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'FlakeAttribute' THEN ba.pdsbatchattribmin END)                AS FlakeAttribute_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'FlakeAttribute' THEN ba.pdsbatchattribmax END)                AS FlakeAttribute_Maximum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Gauge' THEN ba.pdsbatchattribtarget END)                      AS Gauge_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Gauge' THEN ba.pdsbatchattribmin END)                         AS Gauge_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Gauge' THEN ba.pdsbatchattribmax END)                         AS Gauge_Maximum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'HI-IV Attribute' THEN ba.pdsbatchattribtarget END)            AS HIIVAttribute_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'HI-IV Attribute' THEN ba.pdsbatchattribmin END)               AS HIIVAttribute_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'HI-IV Attribute' THEN ba.pdsbatchattribmax END)               AS HIIVAttribute_Maximum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'ID Tube' THEN ba.pdsbatchattribtarget END)                    AS IDTube_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'ID Tube' THEN ba.pdsbatchattribmin END)                       AS IDTube_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'ID Tube' THEN ba.pdsbatchattribmax END)                       AS IDTube_Maximum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Lift Weight' THEN ba.pdsbatchattribtarget END)                AS LiftWeight_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Lift Weight' THEN ba.pdsbatchattribmin END)                   AS LiftWeight_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Lift Weight' THEN ba.pdsbatchattribmax END)                   AS LiftWeight_Maximum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Mn' THEN ba.pdsbatchattribtarget END)                         AS Manganese_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Mn' THEN ba.pdsbatchattribmin END)                            AS Manganese_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Mn' THEN ba.pdsbatchattribmax END)                            AS Manganese_Maximum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Mo' THEN ba.pdsbatchattribtarget END)                         AS Molybdenum_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Mo' THEN ba.pdsbatchattribmin END)                            AS Molybdenum_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Mo' THEN ba.pdsbatchattribmax END)                            AS Molybdenum_Maximum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'N' THEN ba.pdsbatchattribtarget END)                          AS Nitrogen_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'N' THEN ba.pdsbatchattribmin END)                             AS Nitrogen_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'N' THEN ba.pdsbatchattribmax END)                             AS Nitrogen_Maximum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Nb' THEN ba.pdsbatchattribtarget END)                         AS Niobium_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Nb' THEN ba.pdsbatchattribmin END)                            AS Niobium_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Nb' THEN ba.pdsbatchattribmax END)                            AS Niobium_Maximum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Ni' THEN ba.pdsbatchattribtarget END)                         AS Nickel_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Ni' THEN ba.pdsbatchattribmin END)                            AS Nickel_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Ni' THEN ba.pdsbatchattribmax END)                            AS Nickel_Maximum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'OD Tube' THEN ba.pdsbatchattribtarget END)                    AS ODTube_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'OD Tube' THEN ba.pdsbatchattribmin END)                       AS ODTube_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'OD Tube' THEN ba.pdsbatchattribmax END)                       AS ODTube_Maximum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Order Thickness Nom' THEN ba.pdsbatchattribtarget END)        AS OrderThicknessNom_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Order Thickness Nom' THEN ba.pdsbatchattribmin END)           AS OrderThicknessNom_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Order Thickness Nom' THEN ba.pdsbatchattribmax END)           AS OrderThicknessNom_Maximum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'P' THEN ba.pdsbatchattribtarget END)                          AS Phosphorus_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'P' THEN ba.pdsbatchattribmin END)                             AS Phosphorus_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'P' THEN ba.pdsbatchattribmax END)                             AS Phosphorus_Maximum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Rec Tube Ht' THEN ba.pdsbatchattribtarget END)                AS RecTubeHeight_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Rec Tube Ht' THEN ba.pdsbatchattribmin END)                   AS RecTubeHeight_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Rec Tube Ht' THEN ba.pdsbatchattribmax END)                   AS RecTubeHeight_Maximum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Rec Tube Width' THEN ba.pdsbatchattribtarget END)             AS RecTubeWidth_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Rec Tube Width' THEN ba.pdsbatchattribmin END)                AS RecTubeWidth_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Rec Tube Width' THEN ba.pdsbatchattribmax END)                AS RecTubeWidth_Maximum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Rockwell Hdness HRC' THEN ba.pdsbatchattribtarget END)        AS RockwellHardnessHRC_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Rockwell Hdness HRC' THEN ba.pdsbatchattribmin END)           AS RockwellHardnessHRC_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Rockwell Hdness HRC' THEN ba.pdsbatchattribmax END)           AS RockwellHardnessHRC_Maximum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Roughness' THEN ba.pdsbatchattribtarget END)                  AS Roughness_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Roughness' THEN ba.pdsbatchattribmin END)                     AS Roughness_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Roughness' THEN ba.pdsbatchattribmax END)                     AS Roughness_Maximum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Si' THEN ba.pdsbatchattribtarget END)                         AS Silicon_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Si' THEN ba.pdsbatchattribmin END)                            AS Silicon_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Si' THEN ba.pdsbatchattribmax END)                            AS Silicon_Maximum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'S' THEN ba.pdsbatchattribtarget END)                          AS Sulfur_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'S' THEN ba.pdsbatchattribmin END)                             AS Sulfur_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'S' THEN ba.pdsbatchattribmax END)                             AS Sulfur_Maximum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Thickness' THEN ba.pdsbatchattribtarget END)                  AS Thickness_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Thickness' THEN ba.pdsbatchattribmin END)                     AS Thickness_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Thickness' THEN ba.pdsbatchattribmax END)                     AS Thickness_Maximum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Sn' THEN ba.pdsbatchattribtarget END)                         AS Tin_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Sn' THEN ba.pdsbatchattribmin END)                            AS Tin_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Sn' THEN ba.pdsbatchattribmax END)                            AS Tin_Maximum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Ti' THEN ba.pdsbatchattribtarget END)                         AS Titanium_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Ti' THEN ba.pdsbatchattribmin END)                            AS Titanium_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Ti' THEN ba.pdsbatchattribmax END)                            AS Titanium_Maximum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Tube OD' THEN ba.pdsbatchattribtarget END)                    AS TubeOD_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Tube OD' THEN ba.pdsbatchattribmin END)                       AS TubeOD_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Tube OD' THEN ba.pdsbatchattribmax END)                       AS TubeOD_Maximum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'V' THEN ba.pdsbatchattribtarget END)                          AS Vanadium_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'V' THEN ba.pdsbatchattribmin END)                             AS Vanadium_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'V' THEN ba.pdsbatchattribmax END)                             AS Vanadium_Maximum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Wall Thickness Tube' THEN ba.pdsbatchattribtarget END)        AS WallThicknessTube_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Wall Thickness Tube' THEN ba.pdsbatchattribmin END)           AS WallThicknessTube_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Wall Thickness Tube' THEN ba.pdsbatchattribmax END)           AS WallThicknessTube_Maximum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Yield' THEN ba.pdsbatchattribtarget END)                      AS Yield_Target
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Yield' THEN ba.pdsbatchattribmin END)                         AS Yield_Minimum
         , MAX(CASE WHEN ba.pdsbatchattribrelation = 'Yield' THEN ba.pdsbatchattribmax END)                         AS Yield_Maximum
         , MAX(ba._recid)                                                                                           AS _RecID
         , 1                                                                                                        AS _SourceID
         ,CURRENT_TIMESTAMP                                               AS _CreatedDate
        , CURRENT_TIMESTAMP                                               AS _ModifiedDate

      FROM productspecattributestage         ba
     INNER JOIN silver.cma_Product dp
        ON dp.LegalEntityID = ba.legalentityid
       AND dp.ItemID        = ba.pdsbatchattribitemid
     GROUP BY dp.ProductKey;
