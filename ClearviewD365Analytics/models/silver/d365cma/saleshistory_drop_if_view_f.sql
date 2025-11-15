{{ config(materialized='table', tags=['silver'], alias='saleshistory_dimrop_if_view_fact') }}

-- Source file: cma/cma/layers/_base/_silver/saleshistory_f/saleshistory_f.py
-- Root method: SaleshistoryFact.drop_if_view

IF NOT EXISTS(SELECT *  FROM sys.external_tables WHERE name = 'cma_SalesHistory_Fact')
BEGIN
  DROP VIEW IF EXISTS silver.cma_SalesHistory_Fact;
END
