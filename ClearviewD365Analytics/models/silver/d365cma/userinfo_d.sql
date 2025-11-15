{{ config(materialized='table', tags=['silver'], alias='userinfo_dim') }}

-- Source file: cma/cma/layers/_base/_silver/userinfo/userinfo.py
-- Root method: Userinfo.userinfodetail [UserInfoDetail]
-- external_table_name: UserInfoDetail
-- schema_name: temp

SELECT  ROW_NUMBER() OVER (ORDER BY t.UserName) AS UserInfoKey
         , CURRENT_TIMESTAMP                                                            AS _CreatedDate
         , CURRENT_TIMESTAMP                                                            AS _ModifiedDate,
         * FROM (
    SELECT DISTINCT
           uf.name    AS CreatedBy
         , uf.company AS LegalEntityID
         , uf.fno_id      AS UserName


      FROM {{ ref('userinfo') }} uf) t;
