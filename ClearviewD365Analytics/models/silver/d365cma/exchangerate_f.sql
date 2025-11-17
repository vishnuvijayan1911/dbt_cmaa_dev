{{ config(materialized='table', tags=['silver'], alias='exchangerate_fact') }}

-- Source file: cma/cma/layers/_base/_silver/exchangerate_f/exchangerate_f.py
-- Root method: ExchangerateFact.exchangerate_factdetail [ExchangeRate_FactDetail]
-- Inlined methods: ExchangerateFact.exchangeratefactcurrency [ExchangeRateFactCurrency], ExchangerateFact.exchangeratefactrates [ExchangeRateFactRates], ExchangerateFact.exchangeratefactdailyrates [ExchangeRateFactDailyRates], ExchangerateFact.exchangeratefactdetail1 [ExchangeRateFactDetail1]
-- external_table_name: ExchangeRate_FactDetail
-- schema_name: temp

WITH
exchangeratefactcurrency AS (
    SELECT erc.fromcurrencycode  AS FromCurrID
             , erc.tocurrencycode    AS ToCurrID
             , er.exchangerate / 100 AS ExchRate
             , ert.name              AS ExchType
             , er.validfrom          AS ValidFrom
             , er.validto            AS ValidTo
             , er.recid              AS _RecID

          FROM {{ ref('exchangerate') }}                  er
         INNER JOIN {{ ref('exchangeratecurrencypair') }} erc
            ON erc.recid = er.exchangeratecurrencypair
         INNER JOIN  {{ ref('exchangeratetype') }}          ert
            ON ert.recid = erc.exchangeratetype;
),
exchangeratefactrates AS (
    SELECT *

          FROM (   SELECT FromCurrID
                        , ToCurrID
                        , ExchType
                        , ExchRate
                        , ValidFrom
                        , ValidTo
                        , _RecID
                     FROM exchangeratefactcurrency
                   UNION ALL
                   SELECT ToCurrID                                 AS FromCurrID
                        , FromCurrID                               AS ToCurrID
                        , ExchType                                 AS ExchType
                        , CAST(1.0000 / ExchRate AS DECIMAL(8, 4)) AS ExchRate
                        , ValidFrom
                        , ValidTo
                        , _RecID                                   AS _RecID
                     FROM exchangeratefactcurrency) r;
),
exchangeratefactdailyrates AS (
    SELECT DISTINCT
               r.FromCurrID
             , r.ToCurrID
             , r.ExchType
             , r.ExchRate
             , dd.Date AS ExchDate
             , r._RecID

          FROM exchangeratefactrates        r
         CROSS JOIN {{ ref('date_d') }} dd
         WHERE dd.Date BETWEEN r.ValidFrom AND r.ValidTo
           AND dd.Date BETWEEN (SELECT MIN(ValidFrom) AS FromDate FROM exchangeratefactcurrency) AND DATEADD(d, 1, GETDATE());
),
exchangeratefactdetail1 AS (
    SELECT *

          FROM (   SELECT r.FromCurrID  AS FromCurrencyID
                        , r.ToCurrID    AS ToCurrencyID
                        , r.ExchType    AS ExchangeRateType
                        , r.ExchRate    AS ExchangeRate
                        , d.DateKey     AS ExchangeDateKey
                        , r._RecID      AS _RecID
                        , 1             AS _SourceID
                        , ROW_NUMBER() OVER (PARTITION BY r.FromCurrID, r.ToCurrID, r.ExchDate, r.ExchType
    ORDER BY r.ExchRate, r._RecID DESC) AS RankVal
                     FROM exchangeratefactdailyrates   r
                    INNER JOIN {{ ref('date_d') }} d
                       ON d.Date = r.ExchDate) t
         WHERE t.RankVal = 1;
)
SELECT ROW_NUMBER() OVER (ORDER BY ts._RecID, ts._SourceID) AS ExchangeRateKey
    , ts.FromCurrencyID   AS FromCurrencyID
         , ts.ToCurrencyID     AS ToCurrencyID
         , ts.ExchangeRateType AS ExchangeRateType
         , ts.ExchangeRate     AS ExchangeRate
         , ts.ExchangeDateKey  AS ExchangeDateKey
         , ts._RecID           AS _RecID
         , ts._SourceID        AS _SourceID
         , CURRENT_TIMESTAMP                                                            AS _CreatedDate
         , CURRENT_TIMESTAMP                                                            AS _ModifiedDate  

      FROM exchangeratefactdetail1 ts;
