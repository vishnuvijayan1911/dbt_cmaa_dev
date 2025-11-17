{{ config(materialized='table', tags=['silver'], alias='purchasetype') }}

-- External table: silver.cma_PurchaseType
-- Provides the lookup values for Purchase Type across purchase models.

SELECT PurchaseTypeKey
     , PurchaseTypeID
     , PurchaseType
     , CURRENT_TIMESTAMP AS _CreatedDate
     , CURRENT_TIMESTAMP AS _ModifiedDate
  FROM silver.cma_PurchaseType;

