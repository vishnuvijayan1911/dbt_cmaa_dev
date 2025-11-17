{{ config(materialized='table', tags=['silver'], alias='standarduomconversion') }}

-- Source file: cma/cma/layers/_base/_silver/standarduomconversion/standarduomconversion.py
-- Root method: StandardUomConversion.standard_uom_conversion [StandardUomConversionDetail]
-- external_table_name: StandardUomConversionDetail
-- schema_name: temp

SELECT 
        prd._recid AS _recid,
        prd.productid,
        prd.productkey,
        prd.legalentityid,
        prd.itemid,
        fromuom.symbol AS fromuom,
        touom.symbol AS touom,
        fromuom.unitofmeasureclass as fromuomclassid,
		    touom.unitofmeasureclass as touomclassid,
        uomc.factor,
        prd.productwidth,
        prd.productlength     
      FROM {{ ref('product_d') }} prd
      INNER JOIN {{ ref('unitofmeasureconversion') }} uomc ON uomc.product = prd.productid
      INNER JOIN {{ ref('unitofmeasure') }} fromuom ON fromuom.recid = uomc.fromunitofmeasure
      INNER JOIN {{ ref('unitofmeasure') }} touom ON touom.recid = uomc.tounitofmeasure

