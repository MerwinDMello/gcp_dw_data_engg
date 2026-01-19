export JOBNAME='J_CDM_ADHOC_CA_Communication_Device'

export AC_EXP_SQL_STATEMENT="
							

select '$JOBNAME' || ',' || cast(count(*) as varchar(30)) || ',' as SOURCE_STRING FROM (
	SELECT DISTINCT 
				UNIONS.Communication_Device_Num 
	FROM (SELECT CASE WHEN WorkExtension IS NOT NULL THEN TRIM(WorkPhone) ||':'|| TRIM(WorkExtension)  ELSE TRIM(WorkPhone) END AS  Communication_Device_Num, Full_server_Nm 
		FROM EDWCDM_STAGING.CardioAccess_Contacts_STG 
		UNION
		SELECT TRIM(PagerNum), Full_server_Nm 
		FROM EDWCDM_STAGING.CardioAccess_Contacts_STG 
		UNION
		SELECT TRIM(HomePhone), Full_server_Nm 
		FROM EDWCDM_STAGING.CardioAccess_Contacts_STG 
		UNION
		SELECT TRIM(MobilePhone), Full_server_Nm 
		FROM EDWCDM_STAGING.CardioAccess_Contacts_STG 
		UNION
		SELECT TRIM(FaxNumber), Full_server_Nm 
		FROM EDWCDM_STAGING.CardioAccess_Contacts_STG
		UNION
		SELECT TRIM(PatPhone), Full_server_Nm 
		FROM EDWCDM_STAGING.CardioAccess_Demographics_STG
		UNION
		SELECT TRIM(PatFax), Full_server_Nm 
		FROM EDWCDM_STAGING.CardioAccess_Demographics_STG
		UNION
		SELECT TRIM(PatWPhone), Full_server_Nm 
		FROM EDWCDM_STAGING.CardioAccess_Demographics_STG
		UNION
		SELECT TRIM(PatCPhone), Full_server_Nm 
		FROM EDWCDM_STAGING.CardioAccess_Demographics_STG
		UNION
		SELECT TRIM(Pager), Full_server_Nm 
		FROM EDWCDM_STAGING.CardioAccess_Demographics_STG
	) UNIONS
	WHERE UNIONS.Communication_Device_Num IS NOT NULL
		AND  NOT EXISTS (SELECT 1 FROM EDWCDM.CA_Communication_Device TGT WHERE TGT.Communication_Device_Num=UNIONS.Communication_Device_Num)

) a"

export AC_ACT_SQL_STATEMENT="
select '$JOBNAME'  || ','  || cast(COUNT(*) as varchar(30)) ||',' as SOURCE_STRING FROM (
Select * from  EDWCDM.CA_Communication_Device 
where DW_Last_Update_Date_Time >= (SELECT Max(Job_Start_date_Time) FROM EDWCDM_DMX_AC.etl_job_run WHERE JOB_NAME = '$JOBNAME' AND Job_Start_date_Time IS NOT NULL )) a"

