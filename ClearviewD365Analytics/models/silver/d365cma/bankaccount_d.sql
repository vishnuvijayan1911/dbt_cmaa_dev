{{ config(materialized='table', tags=['silver'], alias='bankaccount') }}

-- Source file: cma/cma/layers/_base/_silver/bankaccount/bankaccount.py
-- Root method: Bankaccount.bankaccountdetail [BankAccountDetail]
-- external_table_name: BankAccountDetail
-- schema_name: temp

SELECT 
           {{ dbt_utils.generate_surrogate_key(['ba.recid']) }} AS BankAccountKey
         , ba.dataareaid                                             AS LegalEntityID
         , ba.accountid                                              AS BankAccountID
         , ba.accountnum                                             AS BankAccountNum
         , CASE WHEN ba.name = '' THEN ba.accountid ELSE ba.name END AS BankAccountName
         , ba.bankgroupid                                            AS BankGroupID
         , ba.currencycode                                           AS CurrencyID
         , ba.recid                                                  AS _RecID
         , 1                                                         AS _SourceID

         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
      FROM {{ ref('bankaccounttable') }} ba

