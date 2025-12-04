{{ config(materialized='table', tags=['silver'], alias='productattribute') }}

-- TODO: replace with real logic for productattribute_d

WITH cma_product_attribute_values AS (
    SELECT
        productid,
        name,
        value
    FROM {{ ref('cmaproductattributevalues') }}
),

attr_pivot AS (
    SELECT
        productid,
        MAX(CASE WHEN name = 'Commodity'                          THEN value END) AS commodity, --todo to replace this with dynamic csv file
        MAX(CASE WHEN name = 'Condition'                          THEN value END) AS condition,
        MAX(CASE WHEN name = 'Customer Number'                    THEN value END) AS customernumber,
        MAX(CASE WHEN name = 'Dimension 1'                        THEN value END) AS formshapedimension1,
        MAX(CASE WHEN name = 'Dimension 2'                        THEN value END) AS formshapedimension2,
        MAX(CASE WHEN name = 'Dimension 3'                        THEN value END) AS formshapedimension3,
        MAX(CASE WHEN name = 'Form/Shape'                        THEN value END) AS formshape,
        MAX(CASE WHEN name = 'Grade/Alloy'                       THEN value END) AS gradealloy,
        MAX(CASE WHEN name = 'Part Number'                       THEN value END) AS partnumber,
        MAX(CASE WHEN name = 'Raw Material'                      THEN value END) AS rawmaterial,
        MAX(CASE WHEN name = 'Revision Number'                   THEN value END) AS revisionnumber,
        MAX(CASE WHEN name = 'Gauge'                             THEN value END) AS gauge,
        MAX(CASE WHEN name = 'Thickness'                         THEN value END) AS thickness,
        MAX(CASE WHEN name = 'Diameter'                          THEN value END) AS diameter,
        MAX(CASE WHEN name = 'Raw Material Classification type' THEN value END) AS rawmaterialclassificationtype
    FROM cma_product_attribute_values
    GROUP BY productid
),

product_no_master AS (
    SELECT *
    FROM {{ ref('d365cma_product_d') }}
    WHERE COALESCE(ProductMasterID, 0) = 0
),

product_with_master AS (
    SELECT *
    FROM {{ ref('d365cma_product_d') }}
    WHERE COALESCE(ProductMasterID, 0) <> 0
),

prd_with_attr_no_master AS (
    SELECT
        p.*,
        ap.*
    FROM product_no_master p
    LEFT JOIN attr_pivot ap
        ON p.ProductID = ap.productid      -- note: ProductID (case) from product_d
),

prd_with_attr_master AS (
    SELECT
        p.*,
        ap.*
    FROM product_with_master p
    LEFT JOIN attr_pivot ap
        ON p.ProductMasterID = ap.productid
),

prd_with_attr AS (
    SELECT * FROM prd_with_attr_no_master
    UNION ALL
    SELECT * FROM prd_with_attr_master
),

cleaned AS (
    SELECT
        p.ProductKey,
        p.ProductID,
        p.ProductName,
        p.ProductMasterID,
        p.LegalEntityID,
        p.ItemID,
        p.ProductCategory,

        -- Not present in final product_d output, commenting out:
        -- p.BaseProduct,
        -- p.BaseProductDesc,

        p.DimensionGroup,
        p.ItemGroupID,
        p.ItemGroup,
        p.ItemModelGroupID,
        p.ItemModelGroup,
        p.ItemTypeID,
        p.ItemType,

        -- These columns don’t exist in product_d final SELECT, comment out:
        -- TRY_CONVERT(datetime2, p.NextRollingDate) AS NextRollingDate,
        -- TRY_CONVERT(decimal(38, 10), p.Price)     AS Price,
        -- TRY_CONVERT(float, p.BonusFactor)         AS BonusFactor,

        -- In product_d final output the column is InventoryUOM (this exists):
        COALESCE(p.InventoryUOM, '')   AS InventoryUOM,

        COALESCE(p.RecoverableScrap, '') AS RecoverableScrap,

        p.StorageGroupID,
        p.StorageGroup,
        p.TrackingGroupID,
        p.TrackingGroup,

        -- These UOM / units exist in product_d:
        p.LengthUnit,
        p.WidthUnit,

        p.ProductColor,
        p.ProductDesc,
        p.ProductLength,
        p.ProductSubTypeID,
        p.ProductSubType,
        p.ProductConfig,
        p.ProductWidth,
        p.Product,

        p.ActivityDate,
        p._SourceID,
        p._RecID,
        p._CreatedDate,
        p._ModifiedDate,

        -- Attribute columns (already brought in via prd_with_attr: p.* includes ap.*)
        COALESCE(p.commodity, '')                     AS commodity,
        COALESCE(p.condition, '')                     AS condition,
        COALESCE(p.customernumber, '')                AS customernumber,
        COALESCE(p.formshapedimension1, '')           AS formshapedimension1,
        COALESCE(p.formshapedimension2, '')           AS formshapedimension2,
        COALESCE(p.formshapedimension3, '')           AS formshapedimension3,
        COALESCE(p.formshape, '')                     AS formshape,
        COALESCE(p.gradealloy, '')                    AS gradealloy,
        COALESCE(p.partnumber, '')                    AS partnumber,
        COALESCE(p.rawmaterial, '')                   AS rawmaterial,
        COALESCE(p.revisionnumber, '')                AS revisionnumber,
        COALESCE(p.gauge, '')                         AS gauge,
        COALESCE(p.thickness, '')                     AS thickness,
        COALESCE(p.diameter, '')                      AS diameter,
        COALESCE(p.rawmaterialclassificationtype, '') AS rawmaterialclassificationtype
    FROM prd_with_attr p
)

SELECT *
FROM cleaned
WHERE ProductKey <> -1;