export JOBNAME='J_CDM_ADHOC_CA_Contact_Detail'

export AC_EXP_SQL_STATEMENT="
select '$JOBNAME' || ',' || cast(count(*) as varchar(30)) || ',' as SOURCE_STRING FROM (
SELECT DISTINCT * FROM (

SELECT CAc.contact_SK AS contact_SK
        ,29 AS contact_Detail_Measure_Id
        ,AParticId AS contact_Detail_Measure_Value_Text
        ,CAST(NULL  AS INTEGER) AS contact_Detail_Measure_Value_Num
FROM EDWCDM_STAGING.CardioAccess_Contacts_STG CACS
        INNER JOIN EDWCDM.CA_SERVER CAS
          ON
                CAS.Server_Name = CAcS.Full_Server_Nm
        INNER JOIN EDWCDM.CA_CONTACT CAc
                ON CAS.Server_SK = CAc.Server_SK
                AND CAcS.ContactID = CAc.Source_contact_Id
WHERE contact_Detail_Measure_Value_Text IS NOT NULL

union 

SELECT CAc.contact_SK AS contact_SK
        ,30 AS contact_Detail_Measure_Id
        ,CParticID AS contact_Detail_Measure_Value_Text
        ,CAST(NULL  AS INTEGER) AS contact_Detail_Measure_Value_Num
FROM EDWCDM_STAGING.CardioAccess_Contacts_STG CACS
        INNER JOIN EDWCDM.CA_SERVER CAS
          ON
                CAS.Server_Name = CAcS.Full_Server_Nm
        INNER JOIN EDWCDM.CA_CONTACT CAc
                ON CAS.Server_SK = CAc.Server_SK
                AND CAcS.ContactID = CAc.Source_contact_Id
WHERE contact_Detail_Measure_Value_Text IS NOT NULL

union

SELECT CAc.contact_SK AS contact_SK
        ,31 AS contact_Detail_Measure_Id
        ,TParticID AS contact_Detail_Measure_Value_Text
        ,CAST(NULL  AS INTEGER)  AS contact_Detail_Measure_Value_Num
FROM EDWCDM_STAGING.CardioAccess_Contacts_STG CACS
        INNER JOIN EDWCDM.CA_SERVER CAS
          ON
                CAS.Server_Name = CAcS.Full_Server_Nm
        INNER JOIN EDWCDM.CA_CONTACT CAc
                ON CAS.Server_SK = CAc.Server_SK
                AND CAcS.ContactID = CAc.Source_contact_Id
WHERE contact_Detail_Measure_Value_Text IS NOT NULL

union

SELECT CAc.contact_SK AS contact_SK
        ,32 AS contact_Detail_Measure_Id
        ,MDNum AS contact_Detail_Measure_Value_Text
        ,CAST(NULL  AS INTEGER)  AS contact_Detail_Measure_Value_Num
FROM EDWCDM_STAGING.CardioAccess_Contacts_STG CACS
        INNER JOIN EDWCDM.CA_SERVER CAS
          ON
                CAS.Server_Name = CAcS.Full_Server_Nm
        INNER JOIN EDWCDM.CA_CONTACT CAc
                ON CAS.Server_SK = CAc.Server_SK
                AND CAcS.ContactID = CAc.Source_contact_Id
WHERE contact_Detail_Measure_Value_Text IS NOT NULL

union 

SELECT CAc.contact_SK AS contact_SK
        ,33 AS contact_Detail_Measure_Id
        ,DEANum AS contact_Detail_Measure_Value_Text
        ,CAST(NULL  AS INTEGER)  AS contact_Detail_Measure_Value_Num
FROM EDWCDM_STAGING.CardioAccess_Contacts_STG CACS
        INNER JOIN EDWCDM.CA_SERVER CAS
          ON
                CAS.Server_Name = CAcS.Full_Server_Nm
        INNER JOIN EDWCDM.CA_CONTACT CAc
                ON CAS.Server_SK = CAc.Server_SK
                AND CAcS.ContactID = CAc.Source_contact_Id
WHERE contact_Detail_Measure_Value_Text IS NOT NULL

union

SELECT CAc.contact_SK AS contact_SK
        ,34 AS contact_Detail_Measure_Id
        ,Dear AS contact_Detail_Measure_Value_Text
        ,CAST(NULL  AS INTEGER)  AS contact_Detail_Measure_Value_Num
FROM EDWCDM_STAGING.CardioAccess_Contacts_STG CACS
        INNER JOIN EDWCDM.CA_SERVER CAS
          ON
                CAS.Server_Name = CAcS.Full_Server_Nm
        INNER JOIN EDWCDM.CA_CONTACT CAc
                ON CAS.Server_SK = CAc.Server_SK
                AND CAcS.ContactID = CAc.Source_contact_Id
WHERE contact_Detail_Measure_Value_Text IS NOT NULL

union 

SELECT CAc.contact_SK AS contact_SK
        ,35 AS contact_Detail_Measure_Id
        , CAST(NULL  AS VARCHAR(100)) AS contact_Detail_Measure_Value_Text
        ,DBCong  AS contact_Detail_Measure_Value_Num
FROM EDWCDM_STAGING.CardioAccess_Contacts_STG CACS
        INNER JOIN EDWCDM.CA_SERVER CAS
          ON
                CAS.Server_Name = CAcS.Full_Server_Nm
        INNER JOIN EDWCDM.CA_CONTACT CAc
                ON CAS.Server_SK = CAc.Server_SK
                AND CAcS.ContactID = CAc.Source_contact_Id
WHERE contact_Detail_Measure_Value_num IS NOT NULL

union 

SELECT CAc.contact_SK AS contact_SK
        ,36 AS contact_Detail_Measure_Id
        ,CAST(NULL  AS VARCHAR(100)) AS contact_Detail_Measure_Value_Text
        ,DBAdlt  AS contact_Detail_Measure_Value_Num
FROM EDWCDM_STAGING.CardioAccess_Contacts_STG CACS
        INNER JOIN EDWCDM.CA_SERVER CAS
          ON
                CAS.Server_Name = CAcS.Full_Server_Nm
        INNER JOIN EDWCDM.CA_CONTACT CAc
                ON CAS.Server_SK = CAc.Server_SK
                AND CAcS.ContactID = CAc.Source_contact_Id
WHERE contact_Detail_Measure_Value_num IS NOT NULL

union 

SELECT CAc.contact_SK AS contact_SK
        ,37 AS contact_Detail_Measure_Id
        ,CAST(NULL  AS VARCHAR(100)) AS contact_Detail_Measure_Value_Text
        ,DBThor  AS contact_Detail_Measure_Value_Num
FROM EDWCDM_STAGING.CardioAccess_Contacts_STG CACS
        INNER JOIN EDWCDM.CA_SERVER CAS
          ON
                CAS.Server_Name = CAcS.Full_Server_Nm
        INNER JOIN EDWCDM.CA_CONTACT CAc
                ON CAS.Server_SK = CAc.Server_SK
                AND CAcS.ContactID = CAc.Source_contact_Id
WHERE contact_Detail_Measure_Value_num IS NOT NULL

union 

SELECT CAc.contact_SK AS contact_SK
        ,38 AS contact_Detail_Measure_Id
        ,CAST(NULL  AS VARCHAR(100)) AS contact_Detail_Measure_Value_Text
        ,PresPhy  AS contact_Detail_Measure_Value_Num
FROM EDWCDM_STAGING.CardioAccess_Contacts_STG CACS
        INNER JOIN EDWCDM.CA_SERVER CAS
          ON
                CAS.Server_Name = CAcS.Full_Server_Nm
        INNER JOIN EDWCDM.CA_CONTACT CAc
                ON CAS.Server_SK = CAc.Server_SK
                AND CAcS.ContactID = CAc.Source_contact_Id
WHERE contact_Detail_Measure_Value_num IS NOT NULL

union 

SELECT CAc.contact_SK AS contact_SK
        ,39 AS contact_Detail_Measure_Id
        ,RxPwd AS contact_Detail_Measure_Value_Text
        ,CAST(NULL  AS INTEGER)  AS contact_Detail_Measure_Value_Num
FROM EDWCDM_STAGING.CardioAccess_Contacts_STG CACS
        INNER JOIN EDWCDM.CA_SERVER CAS
          ON
                CAS.Server_Name = CAcS.Full_Server_Nm
        INNER JOIN EDWCDM.CA_CONTACT CAc
                ON CAS.Server_SK = CAc.Server_SK
                AND CAcS.ContactID = CAc.Source_contact_Id
WHERE contact_Detail_Measure_Value_Text IS NOT NULL

union 

SELECT CAc.contact_SK AS contact_SK
        ,40 AS contact_Detail_Measure_Id
        ,SurgID AS contact_Detail_Measure_Value_Text
        ,CAST(NULL  AS INTEGER)  AS contact_Detail_Measure_Value_Num
FROM EDWCDM_STAGING.CardioAccess_Contacts_STG CACS
        INNER JOIN EDWCDM.CA_SERVER CAS
          ON
                CAS.Server_Name = CAcS.Full_Server_Nm
        INNER JOIN EDWCDM.CA_CONTACT CAc
                ON CAS.Server_SK = CAc.Server_SK
                AND CAcS.ContactID = CAc.Source_contact_Id
WHERE contact_Detail_Measure_Value_Text IS NOT NULL

union 

SELECT CAc.contact_SK AS contact_SK
        ,41 AS contact_Detail_Measure_Id
        ,SurgNPI AS contact_Detail_Measure_Value_Text
        ,CAST(NULL  AS INTEGER)  AS contact_Detail_Measure_Value_Num
FROM EDWCDM_STAGING.CardioAccess_Contacts_STG CACS
        INNER JOIN EDWCDM.CA_SERVER CAS
          ON
                CAS.Server_Name = CAcS.Full_Server_Nm
        INNER JOIN EDWCDM.CA_CONTACT CAc
                ON CAS.Server_SK = CAc.Server_SK
                AND CAcS.ContactID = CAc.Source_contact_Id
WHERE contact_Detail_Measure_Value_Text IS NOT NULL

UNION

SELECT CAc.contact_SK AS contact_SK
        ,42 AS contact_Detail_Measure_Id
        ,TIN AS contact_Detail_Measure_Value_Text
        ,CAST(NULL  AS INTEGER)  AS contact_Detail_Measure_Value_Num
FROM EDWCDM_STAGING.CardioAccess_Contacts_STG CACS
        INNER JOIN EDWCDM.CA_SERVER CAS
          ON
                CAS.Server_Name = CAcS.Full_Server_Nm
        INNER JOIN EDWCDM.CA_CONTACT CAc
                ON CAS.Server_SK = CAc.Server_SK
                AND CAcS.ContactID = CAc.Source_contact_Id
WHERE contact_Detail_Measure_Value_Text IS NOT NULL

union 

SELECT CAc.contact_SK AS contact_SK
        ,43 AS contact_Detail_Measure_Id
        ,UPIN AS contact_Detail_Measure_Value_Text
        ,CAST(NULL  AS INTEGER)  AS contact_Detail_Measure_Value_Num
FROM EDWCDM_STAGING.CardioAccess_Contacts_STG CACS
        INNER JOIN EDWCDM.CA_SERVER CAS
          ON
                CAS.Server_Name = CAcS.Full_Server_Nm
        INNER JOIN EDWCDM.CA_CONTACT CAc
                ON CAS.Server_SK = CAc.Server_SK
                AND CAcS.ContactID = CAc.Source_contact_Id
WHERE contact_Detail_Measure_Value_Text IS NOT NULL

 union 

SELECT CAc.contact_SK AS contact_SK
        ,44 AS contact_Detail_Measure_Id
        ,ContactIDFT AS contact_Detail_Measure_Value_Text
        ,CAST(NULL  AS INTEGER)  AS contact_Detail_Measure_Value_Num
FROM EDWCDM_STAGING.CardioAccess_Contacts_STG CACS
        INNER JOIN EDWCDM.CA_SERVER CAS
          ON
                CAS.Server_Name = CAcS.Full_Server_Nm
        INNER JOIN EDWCDM.CA_CONTACT CAc
                ON CAS.Server_SK = CAc.Server_SK
                AND CAcS.ContactID = CAc.Source_contact_Id
WHERE contact_Detail_Measure_Value_Text IS NOT NULL

union 


SELECT CAc.contact_SK AS contact_SK
        ,45 AS contact_Detail_Measure_Id
        ,ECLSCenterID AS contact_Detail_Measure_Value_Text
        ,CAST(NULL  AS INTEGER)  AS contact_Detail_Measure_Value_Num
FROM EDWCDM_STAGING.CardioAccess_Contacts_STG CACS
        INNER JOIN EDWCDM.CA_SERVER CAS
          ON
                CAS.Server_Name = CAcS.Full_Server_Nm
        INNER JOIN EDWCDM.CA_CONTACT CAc
                ON CAS.Server_SK = CAc.Server_SK
                AND CAcS.ContactID = CAc.Source_contact_Id
WHERE contact_Detail_Measure_Value_Text IS NOT NULL

union 


SELECT CAc.contact_SK AS contact_SK
        ,46 AS contact_Detail_Measure_Id
        ,CAST(NULL  AS VARCHAR(100)) AS contact_Detail_Measure_Value_Text
        ,ProviderId  AS contact_Detail_Measure_Value_Num
FROM EDWCDM_STAGING.CardioAccess_Contacts_STG CACS
        INNER JOIN EDWCDM.CA_SERVER CAS
          ON
                CAS.Server_Name = CAcS.Full_Server_Nm
        INNER JOIN EDWCDM.CA_CONTACT CAc
                ON CAS.Server_SK = CAc.Server_SK
                AND CAcS.ContactID = CAc.Source_contact_Id
WHERE contact_Detail_Measure_Value_num IS NOT NULL

union 

SELECT CAc.contact_SK AS contact_SK
        ,47 AS contact_Detail_Measure_Id
        ,CAST(NULL  AS VARCHAR(100)) AS contact_Detail_Measure_Value_Text
        ,Accredidation  AS contact_Detail_Measure_Value_Num
FROM EDWCDM_STAGING.CardioAccess_Contacts_STG CACS
        INNER JOIN EDWCDM.CA_SERVER CAS
          ON
                CAS.Server_Name = CAcS.Full_Server_Nm
        INNER JOIN EDWCDM.CA_CONTACT CAc
                ON CAS.Server_SK = CAc.Server_SK
                AND CAcS.ContactID = CAc.Source_contact_Id
WHERE contact_Detail_Measure_Value_num IS NOT NULL


)b WHERE TRIM(contact_Detail_Measure_Value_Text) <> ''
                OR contact_Detail_Measure_Value_Num IS NOT NULL)c"

export AC_ACT_SQL_STATEMENT="
select '$JOBNAME'  || ','  || cast(COUNT(*) as varchar(30)) ||',' as SOURCE_STRING FROM (
Select * from  EDWCDM.CA_contact_Detail
where DW_Last_Update_Date_Time >= (SELECT Max(Job_Start_date_Time) FROM EDWCDM_DMX_AC.etl_job_run WHERE JOB_NAME = '$JOBNAME' AND Job_Start_date_Time IS NOT NULL )
) b"
