{{ config(materialized='table', tags=['silver'], alias='ledgeraccount') }}

-- Source file: cma/cma/layers/_base/_silver/ledgeraccount/ledgeraccount.py
-- Root method: Ledgeraccount.ledgeraccountdetail [LedgerAccountDetail]
-- Inlined methods: Ledgeraccount.ledgeraccountdetail1 [LedgerAccountDetail1]
-- external_table_name: LedgerAccountDetail
-- schema_name: temp

WITH
ledgeraccountdetail1 AS (
    SELECT 
             coa.name                                                                   AS ChartOfAccountsID
             , coa.description                                                            AS ChartOfAccounts
             , davc.displayvalue                                                   AS LedgerAccountID
             , ISNULL(we1.enumvalueid, -1)                                                AS LedgerTypeID
             , ISNULL(we1.enumvalue, 'None')                                              AS LedgerType
             , ISNULL(ma.name, 'None')                                                    AS MainAccountName
             , ISNULL(ma.mainaccountid, '00000') + ' - ' + ISNULL(ma.name, 'None')        AS MainAccount
             , MAX(CASE WHEN da.name = 'BusinessUnit' THEN dalv.displayvalue ELSE '' END) AS BusinessUnitID
             , MAX(CASE WHEN da.name = 'Department' THEN dalv.displayvalue ELSE '' END)   AS DepartmentID
             , MAX(CASE WHEN da.name = 'Department' THEN dft.description ELSE '' END)     AS Department
             , MAX(CASE WHEN da.name = 'MainAccount' THEN dalv.displayvalue ELSE '' END)  AS MainAccountID
             , MAX(CASE WHEN da.name = 'MainAccount' THEN dav.issuspended ELSE 0 END)     AS Suspended
             , davc.mainaccount                                                           AS RecID_MA
             , davc.recid                                                                 AS _RecID
             , 1                                                                          AS _SourceID
          FROM {{ ref('dimensionattributevaluecombination') }}           davc
         INNER JOIN {{ ref('dimensionattributevaluegroupcombination') }} davgc
            ON davgc.dimensionattributevaluecombination = davc.recid
         INNER JOIN {{ ref('dimensionattributevaluegroup') }}            davg
            ON davg.recid                               = davgc.dimensionattributevaluegroup
         INNER JOIN {{ ref('dimensionattributelevelvalue') }}            dalv
            ON dalv.dimensionattributevaluegroup        = davg.recid
         INNER JOIN {{ ref('dimensionhierarchy') }}                      dh
            ON dh.recid                                 = davg.dimensionhierarchy
           AND dh.deletedversion                        = 0
           AND dh.isdraft                               = 0
         INNER JOIN {{ ref('dimensionattributevalue') }}                 dav
            ON dav.recid                                = dalv.dimensionattributevalue
          LEFT JOIN {{ ref('dimensionattribute') }}                      da
            ON da.recid                                 = dav.dimensionattribute
           AND da.name IN ( 'BusinessUnit', 'Department', 'MainAccount' )
         INNER JOIN {{ ref('mainaccount') }}                             ma
            ON ma.recid                                 = davc.mainaccount
         INNER JOIN {{ ref('ledgerchartofaccounts') }}                  coa
            ON coa.recid                                = ma.ledgerchartofaccounts
          LEFT JOIN {{ ref('enumeration') }}                             we1
            ON we1.enum                                 = 'LedgerDimensionType'
           AND we1.enumvalueid                          = davc.ledgerdimensiontype
          LEFT JOIN {{ ref('dimensionfinancialtag') }}                   dft
            ON dft.value                                = dalv.displayvalue
         GROUP BY davc.displayvalue
                , ISNULL(ma.name, 'None')
                , ISNULL(ma.mainaccountid, '00000') + ' - ' + ISNULL(ma.name, 'None')
                , davc.recid
                , coa.recid
                , davc.mainaccount
                , coa.name
                , coa.description
                , we1.enumvalueid
                , we1.enumvalue;
)
SELECT ROW_NUMBER() OVER (ORDER BY ta._RecID, ta._SourceID) AS LedgerAccountKey
         , ISNULL(we1.enumvalueid, -1)                                                                  AS MainAccountTypeID
         , ISNULL(we1.enumvalue, '0')                                                                   AS MainAccountType
         , mac.accountcategory                                                                          AS MainAccountCategoryID
         , CASE WHEN mac.description = '' THEN mac.accountcategory ELSE mac.description END             AS MainAccountCategory
         , ROW_NUMBER() OVER (PARTITION BY ta.ChartOfAccountsID, ta.LedgerAccountID
ORDER BY ma.recid)                                                                                     AS AccountRank
         , ta.ChartOfAccountsID                                                                         AS ChartOfAccountsID
         , CASE WHEN ta.ChartOfAccounts = '' THEN ta.ChartOfAccountsID ELSE ta.ChartOfAccounts END      AS ChartOfAccounts
         , ta.LedgerAccountID                                                                           AS LedgerAccountID
         , ta.LedgerTypeID                                                                              AS LedgerTypeID
         , CASE WHEN ta.LedgerType = '' THEN CAST(ta.LedgerTypeID AS VARCHAR(20))ELSE ta.LedgerType END AS LedgerType
         , CASE WHEN ta.MainAccountName = '' THEN ta.MainAccountID ELSE ta.MainAccountName END          AS MainAccountName
         , ta.MainAccount                                                                               AS MainAccount
         , ta.BusinessUnitID                                                                            AS BusinessUnitID
         , ta.DepartmentID                                                                              AS DepartmentID
         , CASE WHEN ta.Department = '' THEN ta.DepartmentID ELSE ta.Department END                     AS Department
         , ta.MainAccountID                                                                             AS MainAccountID
         , ta.Suspended                                                                                 AS Suspended
         , ta._RecID                                                                                    AS _RecID
         , ta._SourceID                                                                                 AS _SourceID

         ,cast(CURRENT_TIMESTAMP as DATETIME2(6))                                               AS _CreatedDate
        , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                               AS _ModifiedDate
      FROM ledgeraccountdetail1                     ta
      LEFT JOIN {{ ref('mainaccount') }}         ma
        ON ma.recid               = ta.RecID_MA
      LEFT JOIN {{ ref('mainaccountcategory') }} mac
        ON mac.accountcategoryref = ma.accountcategoryref
      LEFT JOIN {{ ref('enumeration') }}         we1
        ON we1.enum               = 'DimensionLedgerAccountType'
       AND we1.enumvalueid        = ma.type;

