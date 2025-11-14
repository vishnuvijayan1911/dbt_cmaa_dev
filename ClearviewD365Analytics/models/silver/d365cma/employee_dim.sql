{{ config(materialized='table', tags=['silver']) }}

-- Source file: cma/cma/layers/_base/_silver/employee/employee.py
-- Root method: Employee.employeedetail [EmployeeDetail]
-- Inlined methods: Employee.employeephonedetail [EmployeePhoneDetail], Employee.employeeemaildetail [EmployeeEmailDetail], Employee.employeedetail1 [EmployeeDetail1]
-- external_table_name: EmployeeDetail
-- schema_name: temp

WITH
employeephonedetail AS (
    SELECT t.*
            FROM (   SELECT dpt.recid  AS RecID
                          , lea.locator AS EmployeePhone
                          , ROW_NUMBER() OVER (PARTITION BY dpt.recid
            ORDER BY lea.isprimary DESC)      AS RankVal
                      FROM {{ ref('hcmworker') }}                       hcm
                      INNER JOIN {{ ref('dirpartytable') }}            dpt
                        ON dpt.recid   = hcm.person
                      INNER JOIN {{ ref('dirpartylocation') }}           dpl
                        ON dpl.party    = dpt.recid
                      INNER JOIN {{ ref('logisticselectronicaddress') }} lea
                        ON lea.location = dpl.location
                      WHERE TYPE = 1 
            ) AS t
          WHERE t.RankVal = 1;
),
employeeemaildetail AS (
    SELECT t.*
          FROM (   SELECT dpt.recid  AS RecID
                        , lea.locator AS EmployeeEmail
                        , ROW_NUMBER() OVER (PARTITION BY dpt.recid
          ORDER BY lea.isprimary DESC)      AS RankVal
                     FROM {{ ref('hcmworker') }}                       hcm
                    INNER JOIN  {{ ref('dirpartytable') }}              dpt
                       ON dpt.recid   = hcm.person
                    INNER JOIN {{ ref('dirpartylocation') }}           dpl
                       ON dpl.party    = dpt.recid
                    INNER JOIN {{ ref('logisticselectronicaddress') }} lea
                       ON lea.location = dpl.location
                    WHERE TYPE = 2 
          ) AS t
         WHERE t.RankVal = 1;
),
employeedetail1 AS (
    SELECT t.*
          FROM (   SELECT LTRIM(RTRIM(dpt.name))          AS EmployeeName
                        , LTRIM(RTRIM(lpa.address))       AS EmployeeAddress
                        , LTRIM(RTRIM(tpd.EmployeePhone)) AS EmployeePhone
                        , LTRIM(RTRIM(ted.EmployeeEmail)) AS EmployeeEmail
                        , hcm.personnelnumber             AS PersonnelNumber
                        , CASE WHEN hce.validfrom <= GETDATE()
                                AND hce.validto >= GETDATE()
                               THEN 'Employed'
                               WHEN hce.validfrom > GETDATE()
                               THEN 'Pending'
                               ELSE 'Terminated' END      AS WorkerStatus
                        , jme.logincardno                 AS BadgeID
                        , jpe.price                       AS WorkerRate
                        , hcm.modifieddatetime          AS _SourceDate
                        , hcm.recid                      AS _RecID
                        , 1                               AS _SourceID
                        , ROW_NUMBER() OVER (PARTITION BY hcm.recid
          ORDER BY hcm.recid )                                 AS RankVal 
                     FROM {{ ref('hcmworker') }}                   hcm
                    INNER JOIN  {{ ref('dirpartytable') }}         dpt
                       ON dpt.recid   = hcm.person
                     LEFT JOIN {{ ref('logisticspostaladdress') }} lpa
                       ON lpa.location = dpt.primaryaddresslocation
                      AND (lpa.validto = CAST('2154-12-31 23:59:59.000' AS DATETIME) OR lpa.validto IS NULL)
                     LEFT JOIN {{ ref('hcmemployment') }}          hce
                       ON hce.worker   = hcm.recid
                     LEFT JOIN {{ ref('jmgpayemployee') }}         jpe
                       ON jpe.worker   = hcm.recid
                     LEFT JOIN {{ ref('jmgemployee') }}            jme
                       ON jme.worker   = hcm.recid
                     LEFT JOIN employeephonedetail   tpd
                       ON tpd.RECID    = dpt.recid
                     LEFT JOIN employeeemaildetail   ted
                       ON ted.RECID    = dpt.recid) AS t
          WHERE t.RankVal = 1;
)
SELECT ROW_NUMBER() OVER (ORDER BY ts._RecID, ts._SourceID) AS EmployeeKey
         ,  ts.EmployeeAddress AS EmployeeAddress
         , ts.EmployeeName    AS EmployeeName
         , ts.EmployeePhone   AS EmployeePhone
         , ts.EmployeeEmail   AS EmployeeEmail
         , ts.PersonnelNumber AS PersonnelNumber
         , ts.WorkerStatus    AS WorkerStatus
         , ts.BadgeID         AS BadgeID
         , ts.WorkerRate      AS WorkerRate
         , ts._SourceDate     AS _SourceDate
         , ts._SourceID       AS _SourceID
         , ts._RecID          AS _RecID
        ,CURRENT_TIMESTAMP                                               AS _CreatedDate
        , CURRENT_TIMESTAMP                                               AS _ModifiedDate

      FROM employeedetail1 ts;
