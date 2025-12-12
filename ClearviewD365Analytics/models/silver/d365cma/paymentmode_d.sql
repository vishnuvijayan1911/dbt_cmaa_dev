{{ config(materialized='table', tags=['silver'], alias='paymentmode') }}

-- Source file: cma/cma/layers/_base/_silver/paymentmode/paymentmode.py
-- Root method: Paymentmode.paymentmodedetail [PaymentModeDetail]
-- Inlined methods: Paymentmode.paymentmodepaymode [PaymentModePayMode]
-- external_table_name: PaymentModeDetail
-- schema_name: temp

WITH
paymentmodepaymode AS (
    SELECT t.*
             , ROW_NUMBER() OVER (PARTITION BY t.DATAAREAID, t.PayModeID
    ORDER BY t.PayModeID) AS RankVal
          FROM (   SELECT vpmt.dataareaid AS DATAAREAID
                        , vpmt.paymmode    AS PayModeID
                        , vpmt.name        AS Name
                     FROM {{ ref('vendpaymmodetable') }} vpmt
                   UNION
                   SELECT cpmt.dataareaid AS DATAAREAID
                        , cpmt.paymmode    AS PayModeID
                        , cpmt.name        AS Name
                     FROM {{ ref('custpaymmodetable') }} cpmt) AS t
         WHERE PayModeID <> '';
)
SELECT  {{ dbt_utils.generate_surrogate_key(['t.LegalEntityID', 't.PaymentModeID']) }} AS PaymentModeKey
        * FROM (
            SELECT DISTINCT
           pa.DATAAREAID                                             AS LegalEntityID
         , pa.PayModeID                                              AS PaymentModeID
         , CASE WHEN pa.Name = '' THEN pa.PayModeID ELSE pa.Name END AS PaymentMode


         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
      FROM paymentmodepaymode pa
     WHERE pa.RankVal = 1 ) t;

