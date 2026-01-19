
select '$JOBNAME' || ',' || cast(count(*) as varchar(30)) || ',' as SOURCE_STRING FROM (
select distinct wrk0.Communication_Device_Type_Code
        ,wrk0.Communication_Device_Num
        , wrk0.contactID 
        ,wrk0.Full_server_Nm from
(SELECT DISTINCT
         Communication_Device_Type_Code
        ,Communication_Device_Num
        , contactID 
        ,Full_server_Nm
FROM (
        SELECT contactID, 'W' Communication_Device_Type_Code, CASE WHEN WorkExtension IS NOT NULL THEN TRIM(WorkPhone) ||':'|| TRIM(WorkExtension) ELSE TRIM(WorkPhone) END AS Communication_Device_Num, Full_server_Nm         FROM EDWCDM_STAGING.CardioAccess_contacts_Stg
union
SELECT contactID, 'P', TRIM(PagerNum), Full_server_Nm        FROM EDWCDM_STAGING.CardioAccess_Contacts_STG
union
SELECT contactID, 'H', TRIM(HomePhone), Full_server_Nm        FROM EDWCDM_STAGING.CardioAccess_Contacts_STG
union
SELECT contactID, 'C', TRIM(MobilePhone), Full_server_Nm        FROM EDWCDM_STAGING.CardioAccess_Contacts_STG
union
SELECT contactID, 'F', TRIM(FaxNumber), Full_server_Nm        FROM EDWCDM_STAGING.CardioAccess_Contacts_STG
)UNIONS
WHERE  Communication_Device_Num IS NOT NULL) WRK0
                INNER JOIN EDWCDM.CA_Communication_Device CAD
                        ON WRK0.Communication_Device_Num = CAD.Communication_Device_Num
)b