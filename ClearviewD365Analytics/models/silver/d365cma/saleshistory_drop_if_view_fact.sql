{{ config(materialized='table', tags=['silver']) }}

-- Source file: cma/cma/layers/_base/_silver/saleshistory_fact/saleshistory_fact.py
-- Root method: SaleshistoryFact.drop_if_view

IF NOT EXISTS(SELECT *  FROM sys.external_tables WHERE name = 'cma_SalesHistory_Fact')
BEGIN
  DROP VIEW IF EXISTS silver.cma_SalesHistory_Fact;
END
