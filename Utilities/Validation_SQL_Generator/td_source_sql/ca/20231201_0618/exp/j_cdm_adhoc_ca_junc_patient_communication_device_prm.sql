
							
select '$JOBNAME' || ',' || cast(count(*) as varchar(30)) || ',' as SOURCE_STRING FROM (
	SELECT DISTINCT 
		 UNIONS.Communication_Device_Type_Code
		,UNIONS.Communication_Device_Num
		,UNIONS.PatID
		,UNIONS.Full_server_Nm
	FROM (
		SELECT PatID, 'H' Communication_Device_Type_Code, TRIM(PatPhone) Communication_Device_Num, Full_server_Nm 
		FROM EDWCDM_STAGING.CardioAccess_Demographics_STG
		UNION
		SELECT  PatID, 'F', TRIM(PatFax), Full_server_Nm 
		FROM EDWCDM_STAGING.CardioAccess_Demographics_STG
		UNION
		SELECT PatID, 'W', TRIM(PatWPhone), Full_server_Nm 
		FROM EDWCDM_STAGING.CardioAccess_Demographics_STG
		UNION
		SELECT PatID, 'C', TRIM(PatCPhone), Full_server_Nm 
		FROM EDWCDM_STAGING.CardioAccess_Demographics_STG
		UNION
		SELECT PatID, 'P', TRIM(Pager), Full_server_Nm 
		FROM EDWCDM_STAGING.CardioAccess_Demographics_STG
	)UNIONS
	INNER JOIN EDWCDM.CA_Communication_Device CAD
		ON UNIONS.Communication_Device_Num = CAD.Communication_Device_Num
	WHERE  UNIONS.Communication_Device_Num IS NOT NULL
) a