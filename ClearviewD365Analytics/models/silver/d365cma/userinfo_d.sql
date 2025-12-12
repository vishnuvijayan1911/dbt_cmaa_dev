{{ config(materialized='table', tags=['silver'], alias='userinfo') }}

-- Source file: cma/cma/layers/_base/_silver/userinfo/userinfo.py
-- Root method: Userinfo.userinfodetail [UserInfoDetail]
-- external_table_name: UserInfoDetail
-- schema_name: temp

SELECT  {{ dbt_utils.generate_surrogate_key(['t.UserName']) }} AS UserInfoKey
         * FROM (
    SELECT DISTINCT
           uf.name    AS CreatedBy
         , uf.company AS LegalEntityID
         , uf.fno_id      AS UserName


         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
      FROM {{ ref('userinfo') }} uf) t;

