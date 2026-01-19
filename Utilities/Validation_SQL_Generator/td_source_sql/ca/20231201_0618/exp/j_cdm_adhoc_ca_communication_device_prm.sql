
							
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
) a