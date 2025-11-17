{{ config(materialized='table', tags=['silver'], alias='standardinverseuomconversion') }}

-- Source file: cma/cma/layers/_base/_silver/standardinverseuomconversion/standardinverseuomconversion.py
-- Root method: StandardInverseUomConversion.standard_inverse_uom_conversion [StandardInverseUomConversionDetail]
-- external_table_name: StandardInverseUomConversionDetail
-- schema_name: temp

select  prd.productkey,
              le.legalentitykey,
		          uom1.uomkey    as fromuomkey,
		          uom.uomkey     as touomkey,
              CASE WHEN uomc.factor = 0 then CAST(0 as FLOAT)
		          else CAST(1 AS FLOAT) / CAST(uomc.factor AS FLOAT)
		          END as factor
      from silver.cma_product prd
      inner join {{ ref('unitofmeasureconversion') }} uomc on uomc.product = prd.productid
      inner join {{ ref('unitofmeasure') }} fromuom on fromuom.recid = uomc.fromunitofmeasure
      inner join {{ ref('unitofmeasure') }} touom on touom.recid = uomc.tounitofmeasure
	    inner join silver.cma_uom uom  on lower(fromuom.symbol) = lower(uom.uom)
	    inner join silver.cma_uom uom1 on lower(touom.symbol)   = lower(uom1.uom) 
	    inner join silver.cma_legalentity le
	    on le.legalentityid=prd.legalentityid

